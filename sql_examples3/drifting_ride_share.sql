/*
 * Dremio "Messy Data" Challenge: Drifting Ride Share GPS
 * 
 * Scenario: 
 * Vehicle telemetry with GPS drift.
 * Some Lat/Lon points jump across the globe (e.g. 0,0 or far outside city bounds).
 * 'Ride_Status' has duplicate events (Start, Start, End, End).
 * 
 * Objective for AI Agent:
 * 1. Filter out GPS coordinates outside valid city bounding box.
 * 2. Deduplicate status events: Keep only the first 'Start' and last 'End' per Ride ID.
 * 3. Calculate distance traveled (Euclidean or Haversine approximation).
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Ride_Share_Logs;
CREATE FOLDER IF NOT EXISTS Ride_Share_Logs.Telemetry;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Ride_Share_Logs.Telemetry.GPS_STREAM (
    RIDE_ID VARCHAR,
    TS TIMESTAMP,
    LAT DOUBLE,
    LON DOUBLE,
    STATUS VARCHAR -- 'START', 'MOVING', 'END'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Drift, Duplicates)
-------------------------------------------------------------------------------

-- Normal Ride (NYC)
INSERT INTO Ride_Share_Logs.Telemetry.GPS_STREAM VALUES
('R-100', '2023-01-01 10:00:00', 40.7128, -74.0060, 'START'),
('R-100', '2023-01-01 10:01:00', 40.7130, -74.0065, 'MOVING'),
('R-100', '2023-01-01 10:10:00', 40.7200, -74.0100, 'END');

-- GPS Glitch (Null Island)
INSERT INTO Ride_Share_Logs.Telemetry.GPS_STREAM VALUES
('R-101', '2023-01-01 11:00:00', 40.7128, -74.0060, 'START'),
('R-101', '2023-01-01 11:05:00', 0.0, 0.0, 'MOVING'), -- Glitch
('R-101', '2023-01-01 11:10:00', 40.7200, -74.0100, 'END');

-- Duplicate Events
INSERT INTO Ride_Share_Logs.Telemetry.GPS_STREAM VALUES
('R-102', '2023-01-01 12:00:00', 40.7128, -74.0060, 'START'),
('R-102', '2023-01-01 12:00:01', 40.7128, -74.0060, 'START'), -- Dupe Start
('R-102', '2023-01-01 12:10:00', 40.7200, -74.0100, 'END'),
('R-102', '2023-01-01 12:10:01', 40.7200, -74.0100, 'END'); -- Dupe End

-- Bulk Fill
INSERT INTO Ride_Share_Logs.Telemetry.GPS_STREAM VALUES
('R-103', '2023-01-01 13:00:00', 40.7300, -73.9900, 'START'),
('R-103', '2023-01-01 13:01:00', 40.7310, -73.9910, 'MOVING'),
('R-103', '2023-01-01 13:02:00', 40.7320, -73.9920, 'MOVING'),
('R-103', '2023-01-01 13:03:00', 40.7330, -73.9930, 'MOVING'),
('R-103', '2023-01-01 13:04:00', 40.7340, -73.9940, 'MOVING'),
('R-103', '2023-01-01 13:05:00', 40.7350, -73.9950, 'END'),
('R-104', '2023-01-01 14:00:00', 40.7400, -73.9800, 'START'),
('R-104', '2023-01-01 14:05:00', 999.0, 999.0, 'MOVING'), -- Impossible Lat
('R-104', '2023-01-01 14:10:00', 40.7500, -73.9700, 'END'),
('R-105', '2023-01-01 15:00:00', 34.0522, -118.2437, 'START'), -- LA (Valid, but wrong city if filtered to NYC)
('R-105', '2023-01-01 15:05:00', 34.0522, -118.2437, 'END'),
('R-106', '2023-01-01 16:00:00', 40.0, -74.0, 'START'),
('R-106', '2023-01-01 16:01:00', NULL, NULL, 'MOVING'), -- Nulls
('R-106', '2023-01-01 16:02:00', 40.1, -74.1, 'END'),
('R-107', '2023-01-01 17:00:00', 40.71, -74.01, 'START'),
('R-107', '2023-01-01 17:01:00', 40.711, -74.011, 'MOVING'),
('R-107', '2023-01-01 17:02:00', 40.712, -74.012, 'MOVING'),
('R-107', '2023-01-01 17:02:00', 40.712, -74.012, 'MOVING'), -- Exact Dupe
('R-107', '2023-01-01 17:03:00', 40.713, -74.013, 'END'),
('R-108', '2023-01-01 18:00:00', 40.7, -74.0, 'START'),
('R-109', '2023-01-01 18:05:00', 40.7, -74.0, 'START'),
('R-110', '2023-01-01 18:10:00', 40.7, -74.0, 'START'),
('R-111', '2023-01-01 18:15:00', 40.7, -74.0, 'START'),
('R-112', '2023-01-01 18:20:00', 40.7, -74.0, 'START'),
('R-113', '2023-01-01 19:00:00', 40.8, -73.9, 'START'),
('R-114', '2023-01-01 19:05:00', 40.8, -73.9, 'END'),
('R-115', '2023-01-01 19:10:00', 40.8, -73.9, 'END'), -- No Start
('R-116', '2023-01-01 20:00:00', 51.5074, -0.1278, 'START'), -- London
('R-117', '2023-01-01 20:05:00', 35.6762, 139.6503, 'START'), -- Tokyo
('R-118', '2023-01-01 20:10:00', 48.8566, 2.3522, 'START'), -- Paris
('R-119', '2023-01-01 20:15:00', -33.8688, 151.2093, 'START'), -- Sydney
('R-120', '2023-01-02 10:00:00', 40.7, -74.0, 'START'),
('R-120', '2023-01-02 10:01:00', 40.7, -74.0, 'MOVING'),
('R-120', '2023-01-02 10:02:00', 40.7, -74.0, 'MOVING'),
('R-120', '2023-01-02 10:03:00', 40.7, -74.0, 'MOVING'),
('R-120', '2023-01-02 10:04:00', 40.7, -74.0, 'MOVING'),
('R-120', '2023-01-02 10:05:00', 40.7, -74.0, 'END'),
('R-121', '2023-01-02 11:00:00', 40.7, -74.0, 'START'),
('R-121', '2023-01-02 11:10:00', 40.7, -74.0, 'END'), -- No Moving
('R-122', '2023-01-02 12:00:00', 40.71, -74.01, 'START'),
('R-122', '2023-01-02 12:00:00', 40.71, -74.01, 'START'), -- Exact Timestamp Dupe
('R-123', '2023-01-02 13:00:', 40.7, -74.0, 'START'); -- Bad TS Format? (Not tested here, strict SQL)

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Process the GPS data in Ride_Share_Logs.Telemetry.GPS_STREAM.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Filter Latitude between -90 and 90, Longitude between -180 and 180.
 *     - Remove points at (0,0) (Null Island).
 *     - De-duplicate events: For each RIDE_ID, keep distinct timestamps.
 *  3. Gold: 
 *     - Calculate 'Total Rides' per day.
 *     - Identify 'Ghost Rides' (Rides with no 'MOVING' events).
 *  
 *  Show the SQL."
 */
