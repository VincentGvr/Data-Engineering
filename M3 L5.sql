/*
	M03 Design for Performance
	 L05 Storage Modes

Pre-requisite:
	Compile vSkew
	Compile vCCIHealth

*/




-- 1. Cleanup
IF OBJECT_ID('fctTrip_CCI') IS NOT NULL		DROP TABLE [fctTrip_CCI]
IF OBJECT_ID('fctTrip_CCI_Ordered') IS NOT NULL	DROP TABLE [fctTrip_CCI_Ordered]



-- 2. Create a CCI version of the original trip table (170,261,325 rows)
--	  Review the CCI Statistics - mostly compressed
CREATE TABLE [fctTrip_CCI]
WITH
	(DISTRIBUTION = HASH(DateID),
	 CLUSTERED COLUMNSTORE INDEX )
AS
SELECT *
	FROM trip

SELECT *
	FROM vCCIHealth
	WHERE Table_Name = 'fctTrip_CCI'


	   
-- 3. Add new data from the fctTrip table (393,590 rows)
--	  Review the CCI Stats - small inserts add data to the OPEN rowgroups
INSERT [fctTrip_CCI]
SELECT *
	FROM fctTrip

SELECT *
	FROM vCCIHealth
	WHERE Table_Name = 'fctTrip_CCI'



-- 4. REORGANIZE the Index
--	  Review the CCI Stats - OPEN RGs not changed
SELECT *
	FROM vCCIHealth
	WHERE Table_Name = 'fctTrip_CCI'

ALTER INDEX ALL ON [fctTrip_CCI]
	REORGANIZE

SELECT *
	FROM vCCIHealth
	WHERE Table_Name = 'fctTrip_CCI'


-- 5. REBUILD the Index
--	  Review the CCI Stats - All OPEN RGs now compressed,
SELECT *
	FROM vCCIHealth
	WHERE Table_Name = 'fctTrip_CCI'

ALTER INDEX ALL ON [fctTrip_CCI]
	REBUILD

SELECT *
	FROM vCCIHealth
	WHERE Table_Name = 'fctTrip_CCI'



-- 6. Create an ORDERED CCI version of the trip table,
CREATE TABLE [fctTrip_CCI_Ordered]
WITH
	(DISTRIBUTION = HASH(DateID),
	 CLUSTERED COLUMNSTORE INDEX ORDER (DateID, MedallionID))
AS
SELECT *
	FROM trip

SELECT *
	FROM vCCIHealth
	WHERE Table_Name = 'fctTrip_CCI_Ordered'

ALTER INDEX ALL ON [fctTrip_CCI_Ordered]
	REBUILD

