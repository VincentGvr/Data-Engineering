create external data source velib
with ( location = 'https://penduickadls.blob.core.windows.net/velib/', credential = adls_msi);
go

-- LANDING 

-- STATION INFO 

SELECT station_id AS LB_station_id
	 , name AS LB_name
	 , lat AS FL_lat
	 , lon AS FL_lon
	 , capacity AS ID_capacity
	 , CAST(DATEADD(S, lastUpdatedOther, '1970-01-01') AS DATETIME) AS DT_apitimestamp
	 , tch_extractTimestamp AS ID_tch_extractTimestamp
FROM openrowset(
    BULK ('/landing/station_information/*.parquet'),
    DATA_SOURCE = 'velib',
    FORMAT = 'parquet',
    DATA_COMPRESSION = 'gzip'
) AS station_information

-- STATION STATUS 
SELECT station_id 			ID_station_id 
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
FROM openrowset(
    BULK ('/landing/station_status/*.parquet'),
    DATA_SOURCE = 'velib',
    FORMAT = 'parquet',
    DATA_COMPRESSION = 'gzip'
) AS station_status

-- FINAL

-- station_information

SELECT LB_station_id
	 , LB_name
	 , FL_lat
	 , FL_lon
	 , ID_capacity
	 , DT_apitimestamp
	 , ID_tch_extractTimestamp 
FROM (
	SELECT LB_station_id
		 , LB_name
		 , FL_lat
		 , FL_lon
		 , ID_capacity
		 , DT_apitimestamp
		 , ID_tch_extractTimestamp 
		 , RANK() OVER (PARTITION BY '1' ORDER BY DT_apitimestamp DESC) AS RANK_DATA
	FROM openrowset(
			BULK ('/final/station_information/*.parquet'),
			DATA_SOURCE = 'velib',
			FORMAT = 'parquet',
			DATA_COMPRESSION = 'gzip'
		) AS station_information ) AS RANKED_QUERY
 WHERE RANK_DATA = 1 


 -- VIEW 

 CREATE VIEW vw_station_information AS 
 SELECT LB_station_id
		 , LB_name
		 , FL_lat
		 , FL_lon
		 , ID_capacity
		 , DT_apitimestamp
FROM openrowset(
	BULK ('/final/station_information/station_information_last.parquet'),
	DATA_SOURCE = 'velib',
	FORMAT = 'parquet',
	DATA_COMPRESSION = 'gzip'
) AS station_information 


-- station_status

 SELECT LB_station_id
, ID_num_bikes_available
, ID_num_docks_available
, BT_is_installed
, BT_is_returning
, BT_is_renting
, ID_ebike
, ID_mechanical
, DT_apitimestamp
, DT_lastreported
, ID_tch_extractTimestamp
FROM (
	SELECT LB_station_id
	   	 , ID_num_bikes_available
	   	 , ID_num_docks_available
	   	 , BT_is_installed
	   	 , BT_is_returning
	   	 , BT_is_renting
	   	 , ID_ebike
	   	 , ID_mechanical
	   	 , DT_apitimestamp
	   	 , DT_lastreported
	   	 , ID_tch_extractTimestamp
	   	 , RANK() OVER (PARTITION BY DT_apitimestamp ORDER BY ID_tch_extractTimestamp ASC) AS RANK_DATA
	FROM openrowset(
		BULK ('/temp/station_status/*.parquet'),
		DATA_SOURCE = 'velib',
		FORMAT = 'parquet',
		DATA_COMPRESSION = 'gzip'
	) AS station_information ) AS RANKED_QUERY
 WHERE RANK_DATA = 1 

 CREATE VIEW vw_station_information AS 
 SELECT LB_station_id
		 , LB_name
		 , FL_lat
		 , FL_lon
		 , ID_capacity
		 , DT_apitimestamp
FROM openrowset(
	BULK ('/final/station_information/station_information_last.parquet'),
	DATA_SOURCE = 'velib',
	FORMAT = 'parquet',
	DATA_COMPRESSION = 'gzip'
) AS station_information 

DROP VIEW vw_station_status
  CREATE VIEW vw_station_status AS 
 SELECT LB_station_id
, ID_num_bikes_available
, ID_num_docks_available
, BT_is_installed
, BT_is_returning
, BT_is_renting
, CAST(ID_ebike AS INT) AS ID_ebike
, CAST(ID_mechanical AS INT) AS ID_mechanical
, DT_apitimestamp
, DT_lastreported
, ID_tch_extractTimestamp
FROM openrowset(
	BULK ('/final/station_status/station_status.parquet'),
	DATA_SOURCE = 'velib',
	FORMAT = 'parquet',
	DATA_COMPRESSION = 'gzip'
) AS station_status 