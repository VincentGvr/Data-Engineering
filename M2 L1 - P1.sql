CREATE MASTER KEY;

--
CREATE EXTERNAL DATA SOURCE WWIStorage
WITH
(
    TYPE = Hadoop,
    LOCATION = 'wasbs://wideworldimporters@sqldwholdata.blob.core.windows.net'
);

--
CREATE EXTERNAL FILE FORMAT TextFileFormat
WITH 
(   
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS
     (   
         FIELD_TERMINATOR = '|',
        USE_TYPE_DEFAULT = FALSE 
    )
);

--
CREATE SCHEMA ext;
GO
CREATE SCHEMA wwi;

--
CREATE EXTERNAL TABLE [ext].[dimension_City](
	[City Key] [int] NOT NULL,
	[WWI City ID] [int] NOT NULL,
	[City] [nvarchar](50) NOT NULL,
	[State Province] [nvarchar](50) NOT NULL,
	[Country] [nvarchar](60) NOT NULL,
	[Continent] [nvarchar](30) NOT NULL,
	[Sales Territory] [nvarchar](50) NOT NULL,
	[Region] [nvarchar](30) NOT NULL,
	[Subregion] [nvarchar](30) NOT NULL,
	[Location] [nvarchar](76) NULL,
	[Latest Recorded Population] [bigint] NOT NULL,
	[Valid From] [datetime2](7) NOT NULL,
	[Valid To] [datetime2](7) NOT NULL,
	[Lineage Key] [int] NOT NULL
)
WITH (LOCATION='/v1/dimension_City/',   
    DATA_SOURCE = WWIStorageCafpi,  
    FILE_FORMAT = TextFileFormatCafi,
 	REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
);  

CREATE EXTERNAL TABLE [ext].[dimension_Customer] (
	[Customer Key] [int] NOT NULL,
	[WWI Customer ID] [int] NOT NULL,
	[Customer] [nvarchar](100) NOT NULL,
	[Bill To Customer] [nvarchar](100) NOT NULL,
   	[Category] [nvarchar](50) NOT NULL,
	[Buying Group] [nvarchar](50) NOT NULL,
	[Primary Contact] [nvarchar](50) NOT NULL,
	[Postal Code] [nvarchar](10) NOT NULL,
	[Valid From] [datetime2](7) NOT NULL,
	[Valid To] [datetime2](7) NOT NULL,
	[Lineage Key] [int] NOT NULL
)
WITH (LOCATION='/v1/dimension_Customer/',   
    DATA_SOURCE = WWIStorage,  
    FILE_FORMAT = TextFileFormat,
 	REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
);  

--
CREATE TABLE [wwi].[dimension_City]
WITH
( 
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_City]
OPTION (LABEL = 'CTAS : Load [wwi].[dimension_City]');

CREATE TABLE [wwi].[dimension_Customer]
WITH
( 
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM [ext].[dimension_Customer]
OPTION (LABEL = 'CTAS : Load [wwi].[dimension_Customer]');

--
SELECT
    r.command,
    s.request_id,
    r.status,
    count(distinct input_name) as nbr_files,
    sum(s.bytes_processed)/1024/1024/1024 as gb_processed
FROM 
    sys.dm_pdw_exec_requests r
    INNER JOIN sys.dm_pdw_dms_external_work s
    ON r.request_id = s.request_id
WHERE
    r.[label] = 'CTAS : Load [wwi].[dimension_City]' OR
    r.[label] = 'CTAS : Load [wwi].[dimension_Customer]' 
GROUP BY
    r.command,
    s.request_id,
    r.status
ORDER BY
    nbr_files desc, 
gb_processed desc;
