import logging
import sys
import json
import os
import io
import boto3
import gzip
import sqlalchemy as sql
from sqlalchemy import create_engine


import pandas as pd
import numpy as np

from datetime import datetime, timedelta, timezone


# Configure logging
logger = logging.getLogger()
log_level = os.getenv("LAMBDA_LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)
logger.setLevel(level)

credential_name = os.environ.get('SECRET_NAME','N/A')
bucket_name = os.environ.get('BUCKET_NAME')
compress_file = os.environ.get('COMPRESS_FILE').lower() in ["true","t","yes","y","none","n"]
db_override = None if os.environ.get('DB_OVERRIDE').lower() in ["false","no", "none"] else os.environ.get('DB_OVERRIDE')

cloudwatch = boto3.client('cloudwatch')

logger.debug(f"overriding DB for local testing {db_override}")

def get_period(dt):
    
    interval = (dt.hour // 6) 
    
    median = datetime(dt.year,dt.month,dt.day, interval * 6  ,0 )
    
    
    upper = median + timedelta(hours=3)
    lower = median - timedelta(hours=3)

    return (lower,upper)

def get_engine():
    
    secretsmanager = boto3.client('secretsmanager')

    logging.debug(f"getting secret {credential_name}")

    secret_result = secretsmanager.get_secret_value(
                    SecretId=credential_name
    )

    secret = json.loads(secret_result["SecretString"])

    if db_override:
        secret["host"] = db_override

    con_string = "mysql+pymysql://{username}:{password}@{host}/{dbname}?charset=utf8mb4".format(**secret)

    logging.debug(f"con string {con_string}")

    return create_engine( con_string )

def write_bucket(df,lower):

    dt = (lower+timedelta(hours=3))

    header_str = f"""# TYPE=SYNOP
#An_date= {dt.strftime("%Y%m%d")}
#An_time= {dt.hour}
#An_range=] {lower.hour}  to  {(lower+timedelta(hours=6)).hour} ]
#StatusFlag: 0(Used);1(Not Used);2(Rejected by DA);3(Never Used by DA);4(Data Thinned);5(Rejected before DA);6(Alternative Used);7(Quality Issue);8(Other Reason);9(No content)
#"""

    file_name = "SYNOP/{}/{}/SYNOP_WIS_{}.csv.gz".format( dt.strftime("%Y"), dt.strftime("%m") , dt.strftime("%Y%m%d_%H") )

    client = boto3.client('s3')

    bio = io.BytesIO()

    with gzip.GzipFile(fileobj=bio,mode="w") as g:

        g.write(header_str.encode("utf8"))
        df.to_csv(g,index=False,encoding="utf8")

    logger.info(f"writing {len(df)} lines to {file_name} in {bucket_name}")

    bio.seek(0)
    client.put_object(Body=bio.getvalue(),Bucket=bucket_name,Key=file_name)

    return


def get_data(lower,upper):

    engine = get_engine()

    obs_cols = ["observed_property_air_temperature", "observed_property_dewpoint_temperature",
    "observed_property_wind_direction","observed_property_wind_speed", "observed_property_relative_humidity",
    "observed_property_pressure_reduced_to_mean_sea_level", "observed_property_non_coordinate_pressure"]

    query = f"""SELECT wsi,result_time,
    {",".join(obs_cols)} ,
    property_data_id, geom_lat, geom_lon, meta_received_datetime
    FROM surfaceobservations WHERE meta_received_datetime >= '{lower}' and meta_received_datetime < '{upper}'
    AND ( {" IS NOT NULL OR ".join(obs_cols)}  )
    """

    logging.debug(query)

    df = pd.read_sql(query,engine)

    logging.info("read %d lines, size %.0f kb",len(df),df.memory_usage(index=True).sum()/(1024) )

    cols = ['wsi', 'result_time', 'property_data_id' , 'meta_received_datetime',
       'observed_property_pressure_reduced_to_mean_sea_level',
       'observed_property_air_temperature',
       'observed_property_dewpoint_temperature',
       'observed_property_relative_humidity',
       'observed_property_wind_direction', 'observed_property_wind_speed',
       'observed_property_non_coordinate_pressure','geom_lat', 'geom_lon']

    # turn obs columns into boolean
    df[obs_cols] = df[obs_cols].fillna(0) == 1.0
    df = df[cols]

    # equivalent observations
    df["observed_property_pressure_reduced_to_mean_sea_level"] = df.observed_property_pressure_reduced_to_mean_sea_level | df.observed_property_non_coordinate_pressure
    df["observed_property_wind_direction"] = df.observed_property_wind_direction & df.observed_property_wind_speed # FIXME: check if this will be dropped below if 0
    #df["observed_property_air_temperature"] = df.observed_property_air_temperature | df.observed_property_dewpoint_temperature
    df["observed_property_relative_humidity"] = df.observed_property_relative_humidity | df.observed_property_dewpoint_temperature


    df = df.drop(["observed_property_non_coordinate_pressure","observed_property_wind_direction","observed_property_dewpoint_temperature"],axis="columns")

    df = df.set_index(['wsi', 'result_time', 'property_data_id','geom_lat','geom_lon','meta_received_datetime']).stack(dropna=True).to_frame()
    df = df.reset_index().rename(columns={"level_6":"var_id",0:"present"})
    df = df[df.present].drop("present",axis="columns")

    df["var_id"]=df.var_id.map( {"observed_property_pressure_reduced_to_mean_sea_level":110, 
                                "observed_property_air_temperature":39,
                                "observed_property_relative_humidity":58,
                                "observed_property_wind_speed":41
                                } )

    # duplicate wind, since it is reported as 41 and 42
    df_wind_dupe = df[df.var_id==41].copy()
    df_wind_dupe["var_id"] = 42 
    df = pd.concat([df, df_wind_dupe])

    # assign time
    df["Station_id"] = 99999
    df["yyyymmdd"] = df.result_time.dt.strftime(date_format="%Y%m%d")
    df["HHMMSS"] = df.result_time.dt.strftime(date_format="%H%M%S")

    # assign geom

    df["Latitude"] = df["geom_lat"].astype(float).apply(lambda x: '{:.5f}'.format(x))
    df["Longitude"] = df["geom_lon"].astype(float).apply(lambda x: '{:.5f}'.format(x))

    df["StatusFlag"] = 0
    df["Centre_id"] = "WIS"
    df["Bg_dep"] = 99999
    df["CodeType"] = 170

    df["Wigos_Id"] = df["wsi"]

    df["Timeliness"] = (df.meta_received_datetime - df.result_time).dt.total_seconds()

    df = df[~df.var_id.isna()]

    cols = ["Station_id","yyyymmdd","HHMMSS","Latitude","Longitude","StatusFlag","Centre_id","var_id","Bg_dep","CodeType","Wigos_Id","Timeliness"]
    df = df[cols]

    len_inital = len(df)

    df = df.drop_duplicates()
    nr_duplicates = len_inital - len(df)
    logger.info("removed %d duplicate records",nr_duplicates)
    
    send_statistics(nr_duplicates,len(df))

    logger.info("returning %d records",len(df))
    
    return df  

def send_statistics(nr_removed,nr_final):
    response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName': 'DuplicateRecordsRemoved',
                'Dimensions': [
                    {
                        'Name': 'Export',
                        'Value': "numberDuplicates"
                    },
                ],
                'Unit': 'Count',
                'Value': nr_removed
            } ,
            {
                'MetricName': 'TotalRecordsExported',
                'Dimensions': [
                    {
                        'Name': 'Export',
                        'Value': "TotalRecords"
                    },
                ],
                'Unit': 'Count',
                'Value': nr_final
            } ,


        ],
        Namespace = 'WIS2monitoring'
    )

def lambda_handler(event, context):
    """this functions receives kinesis records and extracts information about surface observations from it"""

    logger.debug(f"export function, event {event}")

    #now = datetime.now(tz=timezone.utc)

    relative_date = datetime.fromisoformat(event["time"].replace("Z","+00:00"))

    now = relative_date

    (lower,upper) = get_period( now )

    logger.info(f"creating export on {now} for period {lower} to {upper}")

    write_bucket(get_data(lower,upper),lower)

    return True
    
            
