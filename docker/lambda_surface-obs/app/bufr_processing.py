import logging
import json
import warnings

import pandas as pd

with warnings.catch_warnings():
    warnings.filterwarnings("ignore",message=r"ecCodes .* or higher is recommended")
    from bufr2geojson import __version__, transform as as_geojson

from jsonpath_ng import jsonpath
from jsonpath_ng.ext import parse 

logger = logging.getLogger(__name__)

exp_lat = parse('$.geometry.coordinates[0]')
exp_lon = parse('$.geometry.coordinates[1]')
exp_alt = parse('$.geometry.coordinates[2]')

exp_params_name = parse('$.properties.parameter[?(@.name == "long_station_name" )].description')
exp_params = parse('$.properties.parameter[?(@.name != "long_station_name" )].name')

exp_wsi = parse('$.properties.wigos_station_identifier')
exp_resultdate = parse('$.properties.resultTime')
exp_phendate = parse('$.properties.phenomenonTime')
#exp_obsprop = parse('$.properties.observedProperty')
exp_obsprop = parse('$.properties.name')

variables_of_interest = [
            "non_coordinate_pressure",
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

def extract_info_from_geojson(geojson_record):
    """extract relevant information from a geojson representation of a BUFR collection"""

    logger.debug("extracting from {}".format(geojson_record))
    
    ret = {}
    
    m = exp_lat.find(geojson_record)
    ret["geom_lat"] = m[0].value if m else ""

    m = exp_lon.find(geojson_record)
    ret["geom_lon"] = m[0].value if m else ""

    m = exp_alt.find(geojson_record)
    ret["geom_height"] = m[0].value if m else ""
    
    m = exp_params_name.find(geojson_record)
    ret["long_name"] = m[0].value if m else ""
    
    m = exp_wsi.find(geojson_record)
    ret["wsi"] = m[0].value if m else ""
    
    m = exp_resultdate.find(geojson_record)
    ret["result_time"] = m[0].value if m else ""
    
    m = exp_phendate.find(geojson_record)
    ret["phenomenon_time"] = m[0].value if m else ""
    
    m = exp_obsprop.find(geojson_record)
    ret["observed_property"] = m[0].value if m else ""
    
    return ret

def group_data(data):

    logger.debug("grouping {}".format(json.dumps(data)))

    keys = ["wsi","result_time","phenomenon_time"]

    try:
        df = pd.DataFrame.from_records(data) # TODO: implementation without need to install pandas
        grp = df.groupby(by=keys).agg( { "geom_lat" : "first" , "geom_lon" : "first", "geom_height" : "first", "observed_property" : list } )
    
        ret = grp.reset_index().rename(columns={"observed_property":"observed_properties"}).to_dict('records')

        logger.debug("returning: {}".format(ret))

        return ret
        
    except KeyError as e:
        logger.error("error with "+json.dumps(data))


def extract_variables(records,filter_variables=True):

    logger.debug("extracting variables from: {}".format(records))

    ret = []
    
    for record in records:
        new_record = { k:v for k,v in record.items() if k != "observed_properties" }
        
        if filter_variables:
            for v in variables_of_interest:
                new_record["observed_property_" + v] = v in record["observed_properties"]
        else:
            for v in record["observed_properties"]:
                new_record["observed_property_" + v] = True
                
        new_record["all_observed_properties"] = ",".join(record["observed_properties"])
    
        ret.append(new_record)        

    logger.debug("returning: {}".format(ret))

    return ret


def process_bufr(data):

    logger.debug("processing bufr record")

    geo_bufr = as_geojson(  data  )

    logger.debug("geojson record: {}".format(geo_bufr))

    ret = []

    if geo_bufr:
        
        for collection in geo_bufr:
            for key, item in collection.items():
                data = extract_info_from_geojson(item['geojson'])
                ret.append( data )

        if len(ret)>0:
            ret = extract_variables(group_data(ret),filter_variables=True)
            logger.debug(json.dumps(ret, indent=4))

    return ret