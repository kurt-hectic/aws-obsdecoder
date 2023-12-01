import json
import logging
import os.path
import sys
import base64
import ssl
import urllib3
import io
import warnings

import pandas as pd

import paho.mqtt.client as mqtt_paho

from bufr2geojson import __version__, transform as as_geojson
from jsonpath_ng import jsonpath
from jsonpath_ng.ext import parse 

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

logging.getLogger("bufr2geojson").setLevel(logging.ERROR)


urllib3.disable_warnings()
timeout = urllib3.Timeout(connect=5.0, read=15.0)
http = urllib3.PoolManager(timeout=timeout,cert_reqs='CERT_NONE', assert_hostname=False)


exp_lat = parse('$.geometry.coordinates[0]')
exp_lon = parse('$.geometry.coordinates[1]')
exp_alt = parse('$.geometry.coordinates[2]')

exp_params_name = parse('$.properties.parameter[?(@.name == "long_station_name" )].description')

exp_params = parse('$.properties.parameter[?(@.name != "long_station_name" )].name')

exp_wsi = parse('$.properties.wigos_station_identifier')
exp_resultdate = parse('$.properties.resultTime')
exp_phendate = parse('$.properties.phenomenonTime')

exp_obsprop = parse('$.properties.observedProperty')

variables_of_interest = [
            #"non_coordinate_pressure",
            "pressure_reduced_to_mean_sea_level",
            #"3hour_pressure_change",
            #"characteristic_of_pressure_tendency",
            "air_temperature",
            "dewpoint_temperature",
            "relative_humidity",
            "wind_direction",
            "wind_speed",
            "total_snow_depth"
            #"horizontal_visibility",
            #"cloud_amount",
            #"height_of_base_of_cloud",
            #"cloud_amount",
            #"height_of_base_of_cloud",
            #"present_weather"
]


def extract_info(c):
    """extract relevant information from a geojson represntation of a BUFR collection"""
    
    ret = {}
    
    m = exp_lat.find(c)
    ret["geom_lat"] = m[0].value if m else ""

    m = exp_lon.find(c)
    ret["geom_lon"] = m[0].value if m else ""

    m = exp_alt.find(c)
    ret["geom_height"] = m[0].value if m else ""
    
    m = exp_params_name.find(c)
    ret["long_name"] = m[0].value if m else ""
    
    m = exp_wsi.find(c)
    ret["wsi"] = m[0].value if m else ""
    
    m = exp_resultdate.find(c)
    ret["result_time"] = m[0].value if m else ""
    
    m = exp_phendate.find(c)
    ret["phenomenon_time"] = m[0].value if m else ""
    
    m = exp_obsprop.find(c)
    ret["observed_property"] = m[0].value if m else ""
    
    return ret


def group_data(data):

    keys = ["wsi","result_time","phenomenon_time"]

    try:
        df = pd.DataFrame.from_records(data)
        grp = df.groupby(by=keys).agg( { "geom_lat" : "first" , "geom_lon" : "first", "geom_height" : "first", "observed_property" : list } )
    
        return grp.reset_index().rename(columns={"observed_property":"observed_properties"}).to_dict('records')
        
    except KeyError as e:
        logger.error("error with "+json.dumps(data))
    
    
#result = as_geojson(  base64.b64decode(bufr)  )

#j = json.load(open(r"test.json"))

#bufr = j["properties"]["content"]["value"]

def extract_variables(records,filter_variables=True):

    ret = []
    
    for record in records:
        new_record = { k:v for k,v in record.items() if k != "observed_properties" }
        
        if filter_variables:
            found_variable_of_interest = False
            for v in variables_of_interest:
                new_record["observed_property_" + v] = v in record["observed_properties"]
                found_variable_of_interest = True
        else:
            for v in record["observed_properties"]:
                new_record["observed_property_" + v] = True
                
        new_record["all_observed_properties"] = ",".join(record["observed_properties"])
    
        ret.append(new_record)        
        
    return ret

def process_bufr(fh):

    geo_bufr = as_geojson(  fh.read()  )

    if geo_bufr:

        ret = []

        for collection in geo_bufr:
            for key, item in collection.items():
                data = extract_info(item['geojson'])
                ret.append( data )

        if len(ret)>0:
            ret = extract_variables(group_data(ret),filter_variables=True)
            logger.info(json.dumps(ret, indent=4))
       

def on_connect_wis(client, userdata, flags, rc):
    """
    set the bad connection flag for rc >0, Sets onnected_flag if connected ok
    also subscribes to topics
    """
    client.subscribe("cache/a/wis2/+/+/data/core/weather/surface-based-observations/synop")
    
    
def handle_content(notification):
    """ this functions returns content to which the notification corresponds """ 

    # get the content. If it is embedded, we use this, otherwise fetch it
    if "content" in notification["properties"] and "value" in notification["properties"]["content"]:
        content = base64.b64decode(notification["properties"]["content"]["value"])       
    else:
        url = [ l["href"] for l in notification["links"] if l["rel"] == "canonical" ][0]
        resp =  http.request('GET', url) 
        
        if resp:
            content = resp.data
        else:
            content = None
    
    return content
    
def on_message(client, userdata, msg):
    topic=msg.topic
    logger.info(f"message received with topic {topic}")
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    
    wis2_notification = json.loads(m_decode)
    
    #print(json.dumps(wis2_notification,indent=4))
    
    content = handle_content(wis2_notification)
    
    if content:
    
        process_bufr(io.BytesIO(content))
    
    #logging.debug("message received")
    #message_routing(client,topic,m_decode)

def create_wis2_connection():
    
    client = mqtt_paho.Client(client_id="test_timo", transport="websockets",
         protocol=mqtt_paho.MQTTv311, clean_session=False)
                         
    client.username_pw_set("everyone", "everyone")
    client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED)
    
    client.on_message = on_message
    client.on_connect = on_connect_wis
    #client.on_subscribe = on_subscribe
    #client.on_disconnect = on_disconnect
    
    #properties=Properties(PacketTypes.CONNECT)
    #properties.SessionExpiryInterval=30*60 # in seconds
    
    client.connect("globalbroker.meteo.fr",
                   #port=8883,
                   port=443,
                   #clean_start=mqtt_paho.MQTT_CLEAN_START_FIRST_ONLY,
                   #properties=properties,
                   keepalive=60)
    
    return client   
    

client = create_wis2_connection()
client.loop_forever()

#with open(r"/test_data/A_ISIA21EIDB202100_C_EDZW_20220320210902_11839953.bin", 'rb') as fh:
#    process_bufr(fh)