import logging
import os
import boto3
import botocore
import time
import datetime
import json

from .bufr_processing import process_bufr

from wis2mon_lib import handle_content, serialize_wis2_message, kinesis_lambda_handler


# Configure logging
logger = logging.getLogger()
log_level = os.getenv("LAMBDA_LOG_LEVEL", "INFO")
disable_filter = os.getenv("DISABLE_FILTER","false").lower() in ["true","yes","y","t"]
level = logging.getLevelName(log_level)
logger.setLevel(level)

logging.getLogger("bufr2geojson").setLevel(logging.ERROR)

firehose_name = os.getenv("FIREHOSE_NAME",None)
ddb_name = os.getenv("DDB_NAME",None)

dynamodb_client = boto3.client('dynamodb')
cloudwatch = boto3.client('cloudwatch')

logging.info("filtering disabled %s",disable_filter)

def process_surface_obs(event,stats):
    logger.debug("processing surface obs {}".format(event))

    content,download_time,cache = handle_content(event)
    if content:
        data = process_bufr( content )
        logger.debug("processed bufr.. extracted {} observations".format(len(data)))

        message_info = serialize_wis2_message(event,validate=False,check_cache=False) ##

        stats.put_stats(cache,download_time)

        # add additional information
        for d in data:
            d["meta_broker"] = message_info["meta_broker"]
            d["meta_topic"] = message_info["meta_topic"]
            d["meta_lambda_datetime"] = message_info["meta_lambda_datetime"]
            d["meta_received_datetime"] = message_info["meta_received_datetime"]
            d["property_data_id"] = message_info["property_data_id"]
    else:
        data=[]    


    return data
 


def do_batch_get(batch_keys):

    tries = 0
    max_tries = 5
    sleepy_time = 1  # Start with 1 second of sleep, then exponentially increase.
    retrieved = {key: [] for key in batch_keys} 
    while tries < max_tries:
        response = dynamodb_client.batch_get_item(RequestItems=batch_keys)
        # Collect any retrieved items and retry unprocessed keys.
        for key in response.get("Responses", []):
            retrieved[key] += response["Responses"][key]
        unprocessed = response["UnprocessedKeys"]
        if len(unprocessed) > 0:
            batch_keys = unprocessed
            unprocessed_count = sum(
                [len(batch_key["Keys"]) for batch_key in batch_keys.values()]
            )
            logger.info(
                "%s unprocessed keys returned. Sleep, then retry.", unprocessed_count
            )
            tries += 1
            if tries < max_tries:
                logger.info("Sleeping for %s seconds.", sleepy_time)
                time.sleep(sleepy_time)
                sleepy_time = min(sleepy_time * 2, 32)
        else:
            break

    return retrieved 

def do_batch_insert(data_ids,existing_keys,now_timestamp):
    
    # TODO: use batch insert for new values
    for data_id in data_ids:
        reponse = dynamodb_client.put_item(
            TableName=ddb_name,
            Item={
                "dataid" : { "S" : data_id } ,
                "date" : { "N" : str(now_timestamp) },
                "expirationdate" : {"N" :  str(int((datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=24)).timestamp()))  }
            }
        )
    

def filter_fun_dedup(notifications):

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    now_timestamp = int(now.timestamp())
    yesterday_timestamp = int((now - datetime.timedelta(days=1)).timestamp())

    initial_length = len(notifications)

    # filter out elements without data id
    notifications = [n for n in notifications if  "data_id" in n.get('properties',{})]
    nr_empty = initial_length - len(notifications)
    logger.debug("filtered out %s empty records inside one batch ", nr_empty )

    # filter out possible duplicates inside the batch
    data_ids_in_notifications = [n["properties"]["data_id"] for n in notifications]
    notifications = [n for i,n in enumerate(notifications) if not n["properties"]["data_id"] in data_ids_in_notifications[:i] ]
    nr_duplicate_in_batch = (initial_length+nr_empty)-len(notifications)
    logger.debug("filtered out %s duplicate records inside one batch ", nr_duplicate_in_batch )

    nr_previous_duplicates=0
    if len(notifications) > 0 :
        # check if data id has already been processed (lookup in dyndb)    
        batch_keys = {
            ddb_name : {
                'Keys' : [ { "dataid" : { "S" : n["properties"]["data_id"] } } for n in notifications ]
            }
        }
        try:
            response = do_batch_get(batch_keys)
        except botocore.exceptions.ClientError:
            logger.exception(
                "Couldn't get items from %s, %s", ddb_name, batch_keys
            )
            raise

        logging.debug("dynamodb lookup of %s and reply %s", json.dumps(batch_keys), json.dumps(response) )

        existing_keys = { key["dataid"]["S"]:int(key["date"]["N"]) for key in response[ddb_name] }

        # filter out existing keys if used recently
        previous_length = len(notifications)
        # we take it if it is either not in dynamodb or the date when it was last seen is smaller than yesterday (older than 24h) TODO: should better use the date in the notifcation not the yesterday, which is based on the date of processing, not publishing
        notifications = [ n for n in notifications if not n["properties"]["data_id"] in existing_keys or existing_keys[n["properties"]["data_id"]] < yesterday_timestamp ]  
        nr_previous_duplicates = previous_length-len(notifications)
        logger.debug("filtered out %s previously processed records", nr_previous_duplicates )

        if len(notifications) > 0:
            # insert/update dataids of processed notifications
            try:
                do_batch_insert([ n["properties"]["data_id"] for n in notifications], existing_keys.keys(), now_timestamp)
                logger.info("inserted/updated %s records into dynamodb", len(notifications) )
            except botocore.exceptions.ClientError:
                logger.exception(
                    "Couldn't update items in %s, continuing, duplicates may occurr", ddb_name
                )

    metrics = [ ("empty_id",nr_empty), ("inside_batch",nr_duplicate_in_batch) , ("previously_processed",nr_previous_duplicates) ]
        
    response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName': 'DuplicateRecordsRemoved',
                'Dimensions': [
                    {
                        'Name': 'Duplicates',
                        'Value': name
                    },
                ],
                'Unit': 'Count',
                'Value': val
            } for (name,val) in metrics  ],
        Namespace = 'WIS2monitoring'
    )

    logging.info("returning %d records",len(notifications))
        
    return notifications


def lambda_handler(event, context):
    """this functions receives kinesis records and extracts information about surface observations from it"""

    logger.debug("received event: %s",event)

    filter_fun = None if disable_filter else filter_fun_dedup

    return kinesis_lambda_handler(event,context,process_surface_obs,firehose_name,filter_fun=filter_fun)