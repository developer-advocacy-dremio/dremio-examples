/*
 * Government Emergency Response (911) Demo
 * 
 * Scenario:
 * Analyzing incident response times and resource deployment efficiency.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Emergency;
CREATE FOLDER IF NOT EXISTS RetailDB.Emergency.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Emergency.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Emergency.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Emergency.Bronze.Incidents (
    IncidentID INT,
    CallTime TIMESTAMP,
    DispatchTime TIMESTAMP,
    ArrivalTime TIMESTAMP,
    Type VARCHAR, -- Fire, Medical, Police
    Zone VARCHAR
);

INSERT INTO RetailDB.Emergency.Bronze.Incidents VALUES
(1, '2025-01-01 01:00:00', '2025-01-01 01:02:00', '2025-01-01 01:10:00', 'Medical', 'North'),
(2, '2025-01-01 02:30:00', '2025-01-01 02:35:00', '2025-01-01 02:45:00', 'Police', 'South'),
(3, '2025-01-01 03:00:00', '2025-01-01 03:01:00', '2025-01-01 03:05:00', 'Fire', 'West'),
(4, '2025-01-01 05:00:00', '2025-01-01 05:03:00', '2025-01-01 05:15:00', 'Medical', 'East'),
(5, '2025-01-01 06:15:00', '2025-01-01 06:16:00', '2025-01-01 06:20:00', 'Medical', 'North'),
(6, '2025-01-01 08:00:00', '2025-01-01 08:05:00', '2025-01-01 08:12:00', 'Police', 'South'),
(7, '2025-01-01 10:00:00', '2025-01-01 10:02:00', '2025-01-01 10:08:00', 'Fire', 'North'),
(8, '2025-01-01 12:00:00', '2025-01-01 12:03:00', '2025-01-01 12:15:00', 'Medical', 'West'),
(9, '2025-01-01 14:00:00', '2025-01-01 14:05:00', '2025-01-01 14:20:00', 'Police', 'East'),
(10, '2025-01-01 16:00:00', '2025-01-01 16:01:00', '2025-01-01 16:06:00', 'Medical', 'South'),
(11, '2025-01-01 18:00:00', '2025-01-01 18:04:00', '2025-01-01 18:10:00', 'Fire', 'East');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Emergency.Silver.ResponseTimes AS
SELECT 
    IncidentID,
    Type,
    Zone,
    DATE_DIFF(CAST(DispatchTime AS TIMESTAMP), CAST(CallTime AS TIMESTAMP)) AS DispatchDelaySeconds,
    DATE_DIFF(CAST(ArrivalTime AS TIMESTAMP), CAST(DispatchTime AS TIMESTAMP)) AS TravelTimeSeconds,
    DATE_DIFF(CAST(ArrivalTime AS TIMESTAMP), CAST(CallTime AS TIMESTAMP)) AS TotalResponseSeconds
FROM RetailDB.Emergency.Bronze.Incidents;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Emergency.Gold.ZoneEfficiency AS
SELECT 
    Zone,
    Type,
    AVG(TotalResponseSeconds) / 60.0 AS AvgResponseMinutes,
    COUNT(*) AS IncidentCount
FROM RetailDB.Emergency.Silver.ResponseTimes
GROUP BY Zone, Type;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Calculate the average response time in minutes for 'Medical' incidents in the 'North' zone using RetailDB.Emergency.Gold.ZoneEfficiency."
*/
