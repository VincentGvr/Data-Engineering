/*
	M03 Design for Performance
	 L03 Picking The Right HASH Distribution Key
	 L04 Use Cases for Round-Robin & Replicated  

Pre-requisite:
	Compile vSkew
	
NOTE
	Visual Studio must be used in order to open the EXPLAIN plan output.
	If using SSMS, use the Estimated Query Plan button instead. 

*/


-- 1. Cleanup
IF OBJECT_ID('fctTrip_HackneyLicenseID') IS NOT NULL	DROP TABLE [fctTrip_HackneyLicenseID]
IF OBJECT_ID('fctTrip_MedallionID') IS NOT NULL		DROP TABLE [fctTrip_MedallionID]
IF OBJECT_ID('fctTrip_TripDistance') IS NOT NULL	DROP TABLE [fctTrip_TripDistance]
IF OBJECT_ID('fctTrip_DateID_NULL') IS NOT NULL		DROP TABLE [fctTrip_DateID_NULL]
IF OBJECT_ID('fctTrip_DateID_Dflt') IS NOT NULL		DROP TABLE [fctTrip_DateID_Dflt]
IF OBJECT_ID('dimDate') IS NOT NULL			DROP TABLE [dimDate]
IF OBJECT_ID('fctTrip_RR') IS NOT NULL			DROP TABLE [fctTrip_RR]
IF OBJECT_ID('fctTrip_MedallionID_BIGINT') IS NOT NULL	DROP TABLE [fctTrip_MedallionID_BIGINT]



-- 2. Verify Key Cardinality for HackneyLicenseID and MedallionID columns
SELECT COUNT(DISTINCT HackneyLicenseID) FROM fctTrip
SELECT COUNT(DISTINCT MedallionID) FROM fctTrip



-- 3. Create tables - HASH DISTRIBUTED tables
CREATE TABLE [fctTrip_HackneyLicenseID]
WITH
	(DISTRIBUTION = HASH(HackneyLicenseID),
	 CLUSTERED COLUMNSTORE INDEX )
AS
SELECT *
	FROM fctTrip

CREATE TABLE [fctTrip_MedallionID]
WITH
	(DISTRIBUTION = HASH(MedallionID),
	 CLUSTERED COLUMNSTORE INDEX )
AS
SELECT *
	FROM fctTrip



-- 4. Show Data Movement (1)
--    JOIN on HackneyLicenseID - Single table SHUFFLE MOVE
-- >>>>>> Q (before): Which table will move? 
EXPLAIN
SELECT *
	FROM [fctTrip_HackneyLicenseID] hl
		INNER JOIN [fctTrip_MedallionID] m
			ON hl.HackneyLicenseID = m.HackneyLicenseID



-- 5. Show Data Movement (2)
--    JOIN on PickupGeographyID - Both tables SHUFFLE MOVE
EXPLAIN 
SELECT *
	FROM [fctTrip_HackneyLicenseID] hl
		INNER JOIN [fctTrip_MedallionID] m
			ON hl.PickupGeographyID = m.PickupGeographyID
-- >>>>>> Q (after): Why did both tables MOVE in this query,  but only one in the previous example?



-- 6. Create table on low-cardinality column
SELECT COUNT(DISTINCT CAST([TripDistanceMiles] AS INT)) 
FROM fctTrip

CREATE TABLE [fctTrip_TripDistance]
WITH
	(DISTRIBUTION = HASH(TripDistanceMiles_Round),
	 CLUSTERED COLUMNSTORE INDEX )AS
SELECT  CAST([TripDistanceMiles] AS INT) AS TripDistanceMiles_Round,
		*
	FROM fctTrip

DBCC PDW_SHOWSPACEUSED ('dbo.fctTrip_TripDistance')



-- 7. Create table on NULLABLE column
CREATE TABLE [fctTrip_DateID_NULL]
WITH
	(DISTRIBUTION = HASH(DateID_NULL),
	 CLUSTERED COLUMNSTORE INDEX )AS
SELECT  DateID AS DateID_NULL,
		*
	FROM fctTrip
UNION
SELECT  NULL AS DateID_NULL,
		*
	FROM fctTrip

DBCC PDW_SHOWSPACEUSED ('fctTrip')
DBCC PDW_SHOWSPACEUSED ('fctTrip_DateID_NULL')



-- 8. Create table on Default value
CREATE TABLE [fctTrip_DateID_Dflt]
WITH
	(DISTRIBUTION = HASH(DateID_Dflt),
	 CLUSTERED COLUMNSTORE INDEX )AS
SELECT  DateID AS DateID_Dflt,
		*
	FROM fctTrip
UNION
SELECT  999 AS DateID_Dflt,
		*
	FROM fctTrip

DBCC PDW_SHOWSPACEUSED ('fctTrip')
DBCC PDW_SHOWSPACEUSED ('fctTrip_DateID_Dflt')



-- 9. Create a RR table
CREATE TABLE [fctTrip_RR]
WITH
	(DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX )AS
SELECT  *
	FROM fctTrip

SELECT *
	FROM vSKEW
	WHERE table_name = 'fctTrip_RR'

DBCC PDW_SHOWSPACEUSED ('fctTrip_RR')



-- 10. Simple SELECT on RR table
EXPLAIN
SELECT *
	FROM [fctTrip_RR]



-- 11. Create a REPLICATED copy of the Date table, then JOIN RR table / Repl table
CREATE TABLE [dimDate]
WITH
	(DISTRIBUTION = REPLICATE,
	 CLUSTERED INDEX (DateID) )
AS
SELECT *
	FROM [dbo].[Date]

EXPLAIN
SELECT *
	FROM [fctTrip_RR] rr
		INNER JOIN [dimDate] d
			ON rr.DateID = d.DateID
-- >>>>>> Q (after): Why did we get a BROADCAST Move, when dimDate is Replicated?? 





-- Run the queries below, 
-- wait 2-3 seconds after the SELECT TOP 1 before running the EXPLAIN to show that once dimDate is cached, 
-- there is no data movement
SELECT TOP 1 *
FROM dimDate

EXPLAIN
SELECT *
	FROM [fctTrip_RR] rr
		INNER JOIN [dimDate] d
			ON rr.DateID = d.DateID



-- 12. JOIN RR table / Distr table
EXPLAIN
SELECT *
	FROM [fctTrip_RR] rr
		INNER JOIN [dbo].[fctTrip_MedallionID] m
			ON rr.[MedallionID] = m.[MedallionID]



-- 13. Create a copy of [fctTrip_MedallionID], with the HASH key as BIGINT
--    Show that data movement ocurs when JOINing INT to BIGINT for the same key
CREATE TABLE [fctTrip_MedallionID_BIGINT]
WITH
	(DISTRIBUTION = HASH(MedallionID),
	 CLUSTERED COLUMNSTORE INDEX )AS
SELECT [DateID]
      ,CAST([MedallionID] AS BIGINT) AS [MedallionID]
      ,[HackneyLicenseID]
      ,[PickupTimeID]
      ,[DropoffTimeID]
      ,[PickupGeographyID]
      ,[DropoffGeographyID]
      ,[PickupLatitude]
      ,[PickupLongitude]
      ,[PickupLatLong]
      ,[DropoffLatitude]
      ,[DropoffLongitude]
      ,[DropoffLatLong]
      ,[PassengerCount]
      ,[TripDurationSeconds]
      ,[TripDistanceMiles]
      ,[PaymentType]
      ,[FareAmount]
      ,[SurchargeAmount]
      ,[TaxAmount]
      ,[TipAmount]
      ,[TollsAmount]
      ,[TotalAmount]
  FROM [dbo].[fctTrip_MedallionID]

EXPLAIN
SELECT *
	FROM [fctTrip_MedallionID] m
		INNER JOIN [fctTrip_MedallionID_BIGINT] bi
			ON m.[MedallionID] = bi.[MedallionID]

