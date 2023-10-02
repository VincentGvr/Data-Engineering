
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Ma$terKeyEncrypti0n'

--Credential used to authenticate to External Data Source 
--TO DO: Replace the below secret value with your Azure Blob Storage Account access key (either key 1 or key 2 is acceptable)
DROP DATABASE SCOPED CREDENTIAL ADLSCredentials
CREATE DATABASE SCOPED CREDENTIAL ADLSCredentials
WITH IDENTITY = 'Shared Access Signature',
SECRET = 'w+qPFQss4nEW2rf0454Oq+zWgOXpYF1MBLe032gK98QzfTbv1CgrwhFnJp1tUZzJGmbXsf7/4Q+XA25podYeWg=='
;
DROP EXTERNAL DATA SOURCE BlobStorages
--TO DO: Replace with the container name and the storage account name respectively within the phrase enclosed <>
CREATE EXTERNAL DATA SOURCE BlobStorages
WITH
(
    TYPE = Hadoop,
    LOCATION = 'abfss://synapsesqlpools@penduickadls.dfs.core.windows.net/',
    CREDENTIAL = [ADLSCredentials]
);

--We will load one file which a fact table. This will be a compressed file.
CREATE EXTERNAL FILE FORMAT [compressedcsv]
WITH ( 
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS ( 
	FIELD_TERMINATOR = ',',
	DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss.fffffff',
        STRING_DELIMITER = '',
        USE_TYPE_DEFAULT = False,
		FIRST_ROW = 2
    ),
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
);

--Create a schema for external tables
CREATE SCHEMA [NYTaxiSTG];

--Create the external table
--External table referencing the single compressed file
CREATE EXTERNAL TABLE [NYTaxiSTG].[TripsCompressed]
(
    [VendorID] varchar(10) NULL,
    [tpep_pickup_datetime] datetime2 NULL,
    [tpep_dropoff_datetime] datetime2 NULL,
    [passenger_count] int NULL,
    [trip_distance] float NULL,
    [RateCodeID] int NULL,
    [store_and_fwd_flag] varchar(3) NULL,
    [PULocationID] int NULL,
    [DOLocationID] int NULL,
    [payment_type] int NULL,
    [fare_amount] money NULL,
    [extra] money NULL,
    [mta_tax] money NULL,
    [tip_amount] money NULL,
    [tolls_amount] money NULL,
    [improvement_surcharge] money NULL,
    [total_amount] money NULL
)
WITH
(
    LOCATION = '/M02L02/TripsCompressed.gz',
    DATA_SOURCE = BlobStorages,
    FILE_FORMAT = [compressedcsv]
);



CREATE SCHEMA [NYCTaxi];

CREATE TABLE [NYCTaxi].[TripsCompressed]
WITH
(
	HEAP,
    DISTRIBUTION = ROUND_ROBIN
)
AS SELECT * FROM [NYTaxiSTG].[TripsCompressed]
OPTION (LABEL = 'CTAS:TripsCompressed')
;

--Count of rows
SELECT COUNT(*) FROM [NYCTaxi].[TripsCompressed]

--Preview after load
SELECT TOP 1000 * FROM [NYCTaxi].[TripsCompressed]

SELECT DISTINCT ew.* 
FROM[sys].[dm_pdw_dms_external_work] ew 
JOIN sys.dm_pdw_exec_requests r 
ON r.request_id = ew.request_id
JOIN Sys.dm_pdw_request_steps s
ON r.request_id = s.request_id
WHERE r.[label] = ' CTAS:TripsCompressed'
ORDER BY  start_time ASC, dms_step_index

