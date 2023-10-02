/*
	M03 Design for Performance
	 L07 Views
	 L08 Materialized Views

*/




-- 1. Cleanup
IF OBJECT_ID('dimDate_Fridays') IS NOT NULL		DROP TABLE [dimDate_Fridays]
IF OBJECT_ID('dimDate_x') IS NOT NULL		    DROP TABLE [dimDate_x]
IF OBJECT_ID('vw_Trips') IS NOT NULL		    DROP VIEW [vw_Trips]
IF OBJECT_ID('mv_TripsByZip') IS NOT NULL	    DROP VIEW [mv_TripsByZip]



-- 2. Create a VIEW, use previous copy of fctTrip, and original RR copy of Date
CREATE VIEW vw_Trips AS
	SELECT	d.[DayOfWeek],
			d.[DayName],
			t.[PassengerCount],
			COUNT(*) AS TripCount
		FROM [Date] AS d
			INNER JOIN fctTrip t
				ON d.[DateID] = t.[DateID]
		GROUP BY d.[DayOfWeek],
				 d.[DayName],
	 			 t.[PassengerCount]



-- 3. SELECT from the VIEW
--    Review the EXPLAIN, and LEAVE IT OPEN
EXPLAIN
SELECT *
	FROM vw_Trips
	ORDER BY [DayOfWeek], PassengerCount

SELECT *
	FROM vw_Trips
	ORDER BY [DayOfWeek], PassengerCount



-- 4. Create Filtered copy of Date – Fridays only
CREATE TABLE dimDate_Fridays
WITH
	(DISTRIBUTION = REPLICATE,
	 CLUSTERED INDEX (DateID) )
AS
SELECT *
	FROM [Date]
	WHERE [DayName] = 'Friday'

SELECT top 1 *
    FROM dimDate_Fridays
--  >>>>>> Q: What is the purpose of the SELECT TOP 1 * command?



-- 5. SWAP Date with dimDate_Fridays	
RENAME OBJECT [Date]		    TO Date_Orig
RENAME OBJECT dimDate_Fridays	TO [Date]



-- 6. SELECT from the View
-- Show Explain / Estimated plan for the same query as before, 
-- Compare to the previous EXPLAIN plan, result is now different - different ge=ometry for Date table (Repl vs RR)
EXPLAIN
SELECT *
	FROM vw_Trips
	ORDER BY [DayOfWeek], PassengerCount

SELECT *
	FROM vw_Trips
	ORDER BY [DayOfWeek], PassengerCount



-- 7. SWAP dimDate back
RENAME OBJECT [Date]		    TO dimDate_Fridays
RENAME OBJECT Date_Orig	        TO [Date]



-- 8. Complex join - DISTRIB Incompatible / Data Movement
-- Create BADLY distributed dimDate table
-- >>>>> Q (before): Why is this a badly distributed table ?
CREATE TABLE [dimDate_x]
WITH
	(DISTRIBUTION = HASH([DayOfMonth]),
	 HEAP )
AS
SELECT *
	FROM [dbo].[Date]
 


-- 9. Check for MV Recommendations 
SET RESULT_SET_CACHING OFF
GO

-- Show the EXPLAIN output
EXPLAIN WITH_RECOMMENDATIONS
SELECT c.*
FROM
    (SELECT b.County,
            b.city,
            count(a.dateid) AS total_trips,
            rank() OVER (PARTITION BY b.county
                        ORDER BY count(a.dateid) DESC) AS rank
    FROM dbo.Trip a
    LEFT JOIN dbo.Geography b ON a.[PickupGeographyID] = b.[GeographyID]
    WHERE [State] = 'NY'
    GROUP BY b.County,
            b.city) c
WHERE rank <=3
    AND total_trips > 100
ORDER BY County,
            City,
            total_trips,
            rank


-- Create a MV from the output (tidied up)
CREATE MATERIALIZED VIEW mvTripsByCounty 
WITH (DISTRIBUTION = HASH([County])) AS
SELECT [b].[County],
       [b].[City],
       [b].[State],
       COUNT(*) AS Total_Trips
FROM [dbo].[Trip] a,
     [dbo].[Geography] b
WHERE [b].[GeographyID] = [a].[PickupGeographyID]
GROUP BY [b].[County],
         [b].[City],
         [b].[State]


-- Recheck the query plan, results are now a simple RETURN operation, from the MV
EXPLAIN 
SELECT c.*
FROM
    (SELECT b.County,
            b.city,
            count(a.dateid) AS total_trips,
            rank() OVER (PARTITION BY b.county
                        ORDER BY count(a.dateid) DESC) AS rank
    FROM dbo.Trip a
    LEFT JOIN dbo.Geography b ON a.[PickupGeographyID] = b.[GeographyID]
    WHERE [State] = 'NY'
    GROUP BY b.County,
            b.city) c
WHERE rank <=3
    AND total_trips > 100
ORDER BY County,
            City,
            total_trips,
            rank


