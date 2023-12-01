## testing
### surfaceobs
```
cdk synth && sam build -t cdk.out\ObsdecoderStack.template.json && sam local invoke SurfaceObsLambda -t cdk.out\ObsdecoderStack.template.json -e docker\lambda_surface-obs\test\test_notification.json --env-vars test_env.json
```
### export
```
cdk synth && sam build -t cdk.out\ObsdecoderStack.template.json && sam local invoke exportLambda -t cdk.out\ObsdecoderStack.template.json  --env-vars test_env.json
```

## re-populate export


## DB schema

CREATE TABLE `surfaceobservations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `wsi` varchar(255) DEFAULT NULL,
  `result_time` datetime DEFAULT NULL,
  `phenomenon_time` varchar(255) DEFAULT NULL,
  `geom_lat` float DEFAULT NULL,
  `geom_lon` float DEFAULT NULL,
  `geom_height` float DEFAULT NULL,
  `property_data_id` varchar(255) DEFAULT NULL,
  `observed_property_pressure_reduced_to_mean_sea_level` tinyint(1) DEFAULT NULL,
  `observed_property_air_temperature` tinyint(1) DEFAULT NULL,
  `observed_property_dewpoint_temperature` tinyint(1) DEFAULT NULL,
  `observed_property_relative_humidity` tinyint(1) DEFAULT NULL,
  `observed_property_wind_direction` tinyint(1) DEFAULT NULL,
  `observed_property_wind_speed` tinyint(1) DEFAULT NULL,
  `observed_property_total_snow_depth` tinyint(1) DEFAULT NULL,
  `observed_property_non_coordinate_pressure` tinyint(1) DEFAULT NULL,
  `all_observed_properties` text,
  `meta_broker` varchar(255) DEFAULT NULL,
  `meta_topic` varchar(255) DEFAULT NULL,
  `meta_lambda_datetime` datetime DEFAULT NULL,
  `meta_received_datetime` datetime DEFAULT NULL,
  `date_inserted` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `date_inserted` (`date_inserted`),
  KEY `meta_broker` (`meta_broker`),
  KEY `observed_property_pressure_reduced_to_mean_sea_level` (`observed_property_pressure_reduced_to_mean_sea_level`),
  KEY `observed_property_air_temperature` (`observed_property_air_temperature`),
  KEY `observed_property_dewpoint_temperature` (`observed_property_dewpoint_temperature`),
  KEY `observed_property_relative_humidity` (`observed_property_relative_humidity`),
  KEY `observed_property_wind_direction` (`observed_property_wind_direction`),
  KEY `observed_property_wind_speed` (`observed_property_wind_speed`),
  KEY `observed_property_total_snow_depth` (`observed_property_total_snow_depth`),
  KEY `observed_property_non_coordinate_pressure` (`observed_property_non_coordinate_pressure`),
  KEY `meta_received_datetime` (`meta_received_datetime`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1
