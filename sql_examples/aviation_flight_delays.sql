/*
 * Aviation Flight Operations Demo
 * 
 * Scenario:
 * An airline analyzes flight delays and weather impact.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Improve on-time performance.
 * 
 * Note: Assumes a catalog named 'AviationDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AviationDB;
CREATE FOLDER IF NOT EXISTS AviationDB.Bronze;
CREATE FOLDER IF NOT EXISTS AviationDB.Silver;
CREATE FOLDER IF NOT EXISTS AviationDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Flight Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AviationDB.Bronze.Airports (
    AirportCode VARCHAR,
    City VARCHAR,
    Country VARCHAR
);

CREATE TABLE IF NOT EXISTS AviationDB.Bronze.Flights (
    FlightID VARCHAR,
    Origin VARCHAR,
    Destination VARCHAR,
    ScheduledDep TIMESTAMP,
    ActualDep TIMESTAMP,
    Status VARCHAR -- 'OnTime', 'Delayed', 'Cancelled'
);

CREATE TABLE IF NOT EXISTS AviationDB.Bronze.WeatherConditions (
    AirportCode VARCHAR,
    ReadingTime TIMESTAMP,
    Condition VARCHAR, -- 'Clear', 'Rain', 'Snow', 'Fog'
    WindSpeed DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO AviationDB.Bronze.Airports (AirportCode, City, Country) VALUES
('JFK', 'New York', 'USA'),
('LHR', 'London', 'UK'),
('DXB', 'Dubai', 'UAE'),
('SFO', 'San Francisco', 'USA');

INSERT INTO AviationDB.Bronze.Flights (FlightID, Origin, Destination, ScheduledDep, ActualDep, Status) VALUES
('AA100', 'JFK', 'LHR', '2025-10-01 18:00:00', '2025-10-01 18:05:00', 'OnTime'),
('BA200', 'LHR', 'JFK', '2025-10-01 10:00:00', '2025-10-01 11:30:00', 'Delayed'),
('EK300', 'DXB', 'SFO', '2025-10-02 02:00:00', '2025-10-02 02:10:00', 'OnTime'),
('UA400', 'SFO', 'JFK', '2025-10-02 08:00:00', '2025-10-02 09:45:00', 'Delayed');

INSERT INTO AviationDB.Bronze.WeatherConditions (AirportCode, ReadingTime, Condition, WindSpeed) VALUES
('LHR', '2025-10-01 10:00:00', 'Fog', 5.0), -- Cause of delay?
('SFO', '2025-10-02 08:00:00', 'Rain', 15.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Delay Analysis
-------------------------------------------------------------------------------

-- 2.1 View: Flight_Delays
-- Calculates delay duration in minutes.
CREATE OR REPLACE VIEW AviationDB.Silver.Flight_Delays AS
SELECT
    f.FlightID,
    f.Origin,
    f.Destination,
    f.ScheduledDep,
    f.ActualDep,
    TIMESTAMPDIFF(MINUTE, f.ScheduledDep, f.ActualDep) AS DelayMinutes,
    w.Condition AS OriginWeather
FROM AviationDB.Bronze.Flights f
LEFT JOIN AviationDB.Bronze.WeatherConditions w 
    ON f.Origin = w.AirportCode 
    AND f.ScheduledDep = w.ReadingTime; -- Simplified join

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Route Statistics
-------------------------------------------------------------------------------

-- 3.1 View: Route_Performance
-- Aggregates delays by route.
CREATE OR REPLACE VIEW AviationDB.Gold.Route_Performance AS
SELECT
    Origin,
    Destination,
    COUNT(FlightID) AS TotalFlights,
    AVG(DelayMinutes) AS AvgDelayMinutes,
    SUM(CASE WHEN DelayMinutes > 15 THEN 1 ELSE 0 END) AS LateFlights
FROM AviationDB.Silver.Flight_Delays
GROUP BY Origin, Destination;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Worst Airports):
"Which Origin airport has the highest AvgDelayMinutes in AviationDB.Gold.Route_Performance?"

PROMPT 2 (Weather Impact):
"Show me delays where OriginWeather was 'Fog' or 'Snow'."
*/
