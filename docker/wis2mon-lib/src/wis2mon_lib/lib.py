#import urllib3
import logging
import json
import base64
import requests
import boto3

from datetime import datetime
from jsonschema import validate
from jsonschema.exceptions import ValidationError as JSONSchemaValidationError
import urllib.parse

logger = logging.getLogger(__name__)
cloudwatch = boto3.client('cloudwatch')

#timeout = urllib3.Timeout(connect=5.0, read=15.0)
#http = urllib3.PoolManager(timeout=timeout)

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources
from . import resources
try:
    file = "notification_schema.json"
    inp_file = (pkg_resources.files(resources) / file)
    with inp_file.open("rb") as f:  # or "rt" as text file with universal newlines
        schema = json.load(f)
except AttributeError:
    # Python < PY3.9, fall back to method deprecated in PY3.11.
    schema = json.loads(pkg_resources.read_text(resources, file))

requests_session = requests.Session()



def handle_content(notification):
    """ this functions returns content to which the notification corresponds.
    Returns None if URL does not exist. Returns an exeption if there is a timeout or other connection problem """ 

    download_time = None
    cache = None
    # get the content. If it is embedded, we use this, otherwise fetch it
    if "content" in notification["properties"] and "value" in notification["properties"]["content"]:
        encoding_method =  notification["properties"]["content"].get("encoding","base64").lower()

        if encoding_method == "base64":
            content = base64.b64decode(notification["properties"]["content"]["value"])
        elif encoding_method == "utf8" or encoding_method == "utf-8":
            content = notification["properties"]["content"]["value"].encode("utf8")
        else:
            raise Exception(f"encoding method {encoding_method} not supported")
    else:
        url = [ l["href"] for l in notification["links"] if l["rel"] == "canonical" ][0]
        #resp =  http.request('GET', url) 
        resp = requests_session.request("GET",url, timeout=4)
        logger.debug("downloaded {} in {}".format(url,resp.elapsed))
        download_time=resp.elapsed.total_seconds()*1000
        parsed_url = urllib.parse.urlparse(url)
        cache = parsed_url.netloc

        content =  resp.content if resp else None
    
    return content,download_time,cache


def validate_wis2_message(j):
    try:
        validate(instance=j, schema=schema)
        return True
    except JSONSchemaValidationError as e:
        logger.debug("invalid schema {}".format(j))
        return False


def serialize_wis2_message(notification,validate=True,check_cache=True):

    logger.debug("serializing: " + json.dumps(notification))

    try:
    
        resp = {
            "id": notification.get("id",None),
            "version" : notification.get("version",None),
            "meta_received_datetime" : notification["_meta"]["time_received"],
            "meta_broker" : notification["_meta"]["broker"],
            "meta_topic" : notification["_meta"]["topic"],
            "meta_lambda_datetime" : datetime.now().isoformat(),
            "meta_validates" : validate_wis2_message(notification) if validate else None
        }

        properties =  notification.get("properties",{})
        resp["property_datetime"] = properties.get("datetime",None)
        resp["property_data_id"] = properties.get("data_id",None)
        resp["pubtime"] = properties.get("pubtime",None)

        content = properties.get("content",{}) 
        resp["content_encoding"] = content.get("encoding",None)
        resp["content_size"] = content.get("size",None)

        integrity = properties.get("integrity",{}) 
        resp["integrity_method"] = integrity.get("method",None)
        
        resp["canonical_href"] = None
        resp["canonical_type"] = None
        for l in notification["links"]:
            if l["rel"] == "canonical":
                resp["canonical_href"] = l["href"]
                resp["canonical_type"] = l["type"]

        resp["meta_content_embedded"] = "content" in properties # is the content embedded?

        temp = resp["meta_topic"].split("/")
        resp["meta_source"] = temp[0] if temp else None       
        if resp.get("meta_source",None) == "cache" and check_cache :
            
            url = resp["canonical_href"]
            if url:
                logging.debug("checking URL:" + url)
                try:
                    #request = http.request('HEAD', url) 
                    #resp["meta_cache_status"] = request.status
                    response = requests_session.head(url)
                    resp["meta_cache_status"] = response.status_code
                    resp["meta_remote_content_size"] = response.headers.get("Content-Length",-1)
                    
                except Exception as e:
                    logging.error("cannot download "+url+","+str(e))
                    resp["meta_cache_status"] = -9
                    resp["meta_remote_content_size"] = -9
        else:
            resp["meta_cache_status"] = None
    
    except KeyError as e:
       logger.ERROR("error in serializing {}".format(notification),exc_info=True) 
       raise Exception("processing error in serialization")
    
    return resp