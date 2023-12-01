import logging
import json
import base64
import boto3
import datetime

logger = logging.getLogger(__name__)

cloudwatch = boto3.client('cloudwatch')


class StatsObject():

    stats = None

    def __init__(self):
        self.stats = {}

    def put_stats(self,broker,elapsed):
        if not broker in self.stats:
            self.stats[broker] = []
        self.stats[broker].append(elapsed)

    def get_stats(self):
        return { broker:[i for i in vals if i ] for broker,vals in self.stats.items() if broker }

    def get_stats_content_included(self):
        return { broker:len([i for i in vals if i == None ]) for broker,vals in self.stats.items() }
    
    def get_stats_average_download_by_broker(self):
        return { broker:sum(vals)/len(vals) for broker,vals in self.get_stats().items() if len(vals)>0 }
    
    def get_stats_number_download_by_broker(self):
        return { broker:len(vals) for broker,vals in self.get_stats().items() if len(vals)>0 }
    

class RetryError(Exception):
    pass

class NonRecoverableError(Exception):
    pass


def kinesis_firehose_processor(event,processing_function):
    output = []
    
    for record in event['records']:
        logger.debug("processing "+record['recordId'])
    
        try:
            payload=json.loads(base64.b64decode(record["data"]))
            
            logger.debug("extracting info from: {}".format(payload))
            data = json.dumps(processing_function(payload)) + "\n"
            notification = base64.b64encode(data.encode())
            result = "Ok"
            logger.debug("processed "+record['recordId']+" "+result)
            
        except Exception as e:
            logger.error("error during WIS2 message processing",exc_info=True)
            result = "ProcessingFailed"
            notification = record["data"]

        # out
        output_record = {
            'recordId': record['recordId'],
            'result': result,
            'data': notification
        }

        output.append(output_record)

    return output


        
def kinesis_lambda_handler(event, context, fn, firehose_name, filter_fun = None ):
    """this functions receives kinesis records and extracts information from it"""
    if not event or not "Records" in event:
        return 
    
    start_time = datetime.datetime.now()
    
    logger.debug("event before processing: {}".format(event))    
    logger.info("lambda event {} records".format(len(event['Records'])))
    
    notifications = [ json.loads(base64.b64decode(r["kinesis"]["data"])) for r in event["Records"] ]

    if filter_fun:
        notifications = filter_fun(notifications)

    if len(notifications) == 0:
        return True

    logger.info("extracting info notifications (len %s)",len(notifications))

    output = []
    stats = StatsObject()
    for notification in notifications :
        logger.debug("extracting info from: {}".format(notification))
        data = fn(notification,stats)
        output = output + data 

    logger.info("extracted data (len %s)",len(output))


    end_time = datetime.datetime.now()
    avg_duration = ((end_time-start_time).total_seconds() * 1000 ) / len(notifications)

    metric_data = [
            {
                'MetricName': 'RecordExecutionDurationAverage',
                'Dimensions': [
                    {
                        'Name': 'Pipeline',
                        'Value': fn.__name__
                    },
                ],
                'Unit': 'Milliseconds',
                'Value': avg_duration
            },
            {
                'MetricName': 'RecordsProcessedNumber',
                'Dimensions': [
                    {
                        'Name': 'Pipeline',
                        'Value': fn.__name__
                    },
                ],
                'Unit': 'Count',
                'Value':  len(event["Records"])
            },

        ]
    
    metric_data = metric_data + [  { "MetricName" : "CacheDownloadAverage" , "Dimensions": [ {"Name": "Cache" , "Value" : broker } ] , "Unit" : "Milliseconds", "Value" : avg_time } for broker,avg_time in stats.get_stats_average_download_by_broker().items() ] 
    metric_data = metric_data + [  { "MetricName" : "CacheDownloadSum" , "Dimensions": [ {"Name": "Cache" , "Value" : broker } ] , "Unit" : "Count", "Value" : nr_downloads } for broker,nr_downloads in stats.get_stats_number_download_by_broker().items() ] 



    response = cloudwatch.put_metric_data(
        MetricData = metric_data,
        Namespace = 'WIS2monitoring'
    )

    logger.info('processed %d records in %d seconds, average duration %.2f seconds', len(event["Records"]), (end_time-start_time).total_seconds() ,  avg_duration / 1000 )  
    #logger.info("submitting {} records to firehose".format(len(output)))
        
    if len(output)>0:
        firehose_client = boto3.client('firehose')
        try:
            step = 500 # firehose accepts max 500 items at a time
            for i in range(0,len(output),step):
                response = firehose_client.put_record_batch(
                    DeliveryStreamName=firehose_name,
                    Records=[
                        { "Data" : json.dumps(d) + "\n" } for d in output[i:i+step] 
                    ]
                )

            logger.debug("response {}".format(response))
            logger.info('submitted {} records to firehose'.format(len(output[i:i+step])))
        except Exception as e:
            logger.error("problem inserting records into firehose",exc_info=True)
            raise e
        finally:
            if firehose_client:
                firehose_client.close()
              
    return True