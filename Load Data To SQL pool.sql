CREATE MASTER KEY;

CREATE DATABASE SCOPED CREDENTIAL velibCreds 
WITH 
    IDENTITY = 'velibCreds'
    , SECRET = 'w+qPFQss4nEW2rf0454Oq+zWgOXpYF1MBLe032gK98QzfTbv1CgrwhFnJp1tUZzJGmbXsf7/4Q+XA25podYeWg=='

--DROP EXTERNAL DATA SOURCE velib

CREATE EXTERNAL DATA SOURCE velib
WITH (
      TYPE = HADOOP
    , LOCATION = 'wasbs://velib@penduickadls.blob.core.windows.net'    
    , CREDENTIAL = velibCreds
)

CREATE EXTERNAL FILE FORMAT parquet 
WITH (
    FORMAT_TYPE = PARQUET 
)

--DROP EXTERNAL TABLE ext.velib_station_info

CREATE EXTERNAL TABLE ext.velib_station_info (
    LB_station_id  BIGINT 
	 , name NVARCHAR(100)
	 , lat FLOAT 
	 , lon FLOAT 
	 , capacity BIGINT 
	 , apitimestamp DATETIME2
	 , tch_extractTimestamp NVARCHAR(100)
) WITH (
    LOCATION = '/final/station_information/station_information_last.parquet'
    ,DATA_SOURCE = velib
    , FILE_FORMAT = parquet
)

SELECT COUNT(1) FROM velib_station_info

CREATE SCHEMA frst

CREATE TABLE frst.velib_station_info
WITH (
    DISTRIBUTION = REPLICATE
,   HEAP 
) AS SELECT 
 LB_station_id, name, lat, lon, capacity, apitimestamp 
 FROM ext.velib_station_info

/*
      station_id 			ID_station_id 
	 , num_bikes_available	ID_num_bikes_available
	 , num_docks_available	ID_num_docks_available
	 , is_installed			BT_is_installed
	 , is_returning			BT_is_returning
	 , is_renting			BT_is_renting
	 , JSON_VALUE(JSON_QUERY(num_bikes_available_types, '$[1]'),'$.ebike') AS ID_ebike
	 , JSON_VALUE(JSON_QUERY(num_bikes_available_types, '$[0]'),'$.mechanical') AS ID_mechanical
	 , CAST(DATEADD(S, lastUpdatedOther, '1970-01-01') AS DATETIME) AS DT_apitimestamp
	 , CAST(DATEADD(S, last_reported, '1970-01-01') AS DATETIME) AS DT_lastreported
	 , tch_extractTimestamp AS ID_tch_extractTimestamp
*/

SELECT * FROM sys.tables 

DROP EXTERNAL TABLE ext.velib_station_status
CREATE EXTERNAL TABLE ext.velib_station_status (
      station_id 			BIGINT 
	 , num_bikes_available	INT 
	 , num_docks_available	INT 
	 , is_installed			INT 
	 , is_returning			INT 
	 , is_renting			INT 
     , lastUpdatedOther NVARCHAR(100)
     , last_reported NVARCHAR(100)
      , num_bikes_available_types NVARCHAR(100) 
     , tch_extractTimestamp NVARCHAR(100)
--	 , num_bikes_available_types NVARCHAR(100)
) WITH (
    LOCATION = '/landing/station_status'
    ,DATA_SOURCE = velib
    , FILE_FORMAT = parquet
)

SELECT COUNT(1) FROM ext.velib_station_status

CREATE TABLE frst.velib_station_status
WITH (
    DISTRIBUTION = ROUND_ROBIN
,   HEAP 
) AS SELECT TOP 100
       station_id 			    
	 , num_bikes_available	
	 , num_docks_available	
	 , is_installed			
	 , is_returning			
	 , is_renting		
     , JSON_VALUE(JSON_QUERY(num_bikes_available_types, '$[1]'),'$.ebike') AS ID_ebike	
 FROM ext.velib_station_info


