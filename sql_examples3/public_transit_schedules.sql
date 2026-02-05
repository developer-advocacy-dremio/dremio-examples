/*
 * Dremio "Messy Data" Challenge: Public Transit Schedules
 * 
 * Scenario: 
 * GTFS-style bus data.
 * 3 Tables: ROUTES (Meta), TRIPS (Schedule), STOP_TIMES (Telemetry).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: Link STOP_TIMES to TRIPS and ROUTES.
 * 2. Parse Times: Handle '24:xx' and '25:xx' as next day.
 * 3. Classify Punctuality: Compare Sched vs Actual Arrival.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS City_Transit;
CREATE FOLDER IF NOT EXISTS City_Transit.Bus_Telemetry;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS City_Transit.Bus_Telemetry.ROUTES (
    ROUTE_ID VARCHAR,
    ROUTE_NAME VARCHAR, -- 'Downtown Express'
    AGENCY_ID VARCHAR
);

CREATE TABLE IF NOT EXISTS City_Transit.Bus_Telemetry.TRIPS (
    TRIP_ID VARCHAR,
    ROUTE_ID VARCHAR,
    SERVICE_ID VARCHAR, -- 'Weekday', 'Weekend'
    DIRECTION INT -- 0 or 1
);

CREATE TABLE IF NOT EXISTS City_Transit.Bus_Telemetry.STOP_TIMES (
    TRIP_ID VARCHAR,
    STOP_ID VARCHAR,
    SCHED_ARR VARCHAR, -- '08:00:00', '25:00:00'
    ACT_ARR VARCHAR -- Actual
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- ROUTES (3 Rows)
INSERT INTO City_Transit.Bus_Telemetry.ROUTES VALUES
('R-1', 'Red Line', 'Metro'), ('R-2', 'Blue Line', 'Metro'), ('R-Nite', 'Night Owl', 'Metro');

-- TRIPS (5 Rows)
INSERT INTO City_Transit.Bus_Telemetry.TRIPS VALUES
('T-100', 'R-1', 'Weekday', 0), ('T-101', 'R-1', 'Weekday', 1),
('T-200', 'R-2', 'Weekday', 0), ('T-300', 'R-Nite', 'Daily', 0),
('T-400', 'R-2', 'Weekend', 0);

-- STOP_TIMES (50+ Rows)
INSERT INTO City_Transit.Bus_Telemetry.STOP_TIMES VALUES
-- Normal
('T-100', 'S-1', '08:00:00', '08:00:05'), ('T-100', 'S-2', '08:15:00', '08:15:10'),
('T-100', 'S-3', '08:30:00', '08:30:00'), ('T-100', 'S-4', '08:45:00', '08:45:10'),
-- Early
('T-100', 'S-5', '09:00:00', '08:58:00'), ('T-100', 'S-6', '09:15:00', '09:12:00'),
-- Late
('T-101', 'S-1', '08:05:00', '08:15:00'), ('T-101', 'S-2', '08:20:00', '08:35:00'),
('T-101', 'S-3', '08:35:00', '08:55:00'), ('T-101', 'S-4', '08:50:00', '09:10:00'),
-- Midnight
('T-300', 'S-10', '23:50:00', '23:55:00'), ('T-300', 'S-11', '24:10:00', '24:15:00'),
('T-300', 'S-12', '25:30:00', '25:35:00'), ('T-300', 'S-13', '26:00:00', '02:05:00'),
-- Bunching
('T-200', 'S-20', '10:00:00', '10:15:00'), ('T-400', 'S-20', '10:15:00', '10:16:00'),
-- Bulk Fill
('T-200', 'S-21', '10:15:00', '10:20:00'), ('T-200', 'S-22', '10:30:00', '10:35:00'),
('T-200', 'S-23', '10:45:00', '10:50:00'), ('T-200', 'S-24', '11:00:00', '11:05:00'),
('T-200', 'S-25', '11:15:00', '11:20:00'), ('T-200', 'S-26', '11:30:00', '11:35:00'),
('T-200', 'S-27', '11:45:00', '11:50:00'), ('T-200', 'S-28', '12:00:00', '12:05:00'),
('T-200', 'S-29', '12:15:00', '12:20:00'), ('T-200', 'S-30', '12:30:00', '12:35:00'),
('T-400', 'S-21', '10:30:00', '10:21:00'), ('T-400', 'S-22', '10:45:00', '10:36:00'),
('T-400', 'S-23', '11:00:00', '10:51:00'), ('T-400', 'S-24', '11:15:00', '11:06:00'),
('T-400', 'S-25', '11:30:00', '11:21:00'), ('T-400', 'S-26', '11:45:00', '11:36:00'),
('T-400', 'S-27', '12:00:00', '11:51:00'), ('T-400', 'S-28', '12:15:00', '12:06:00'),
('T-400', 'S-29', '12:30:00', '12:21:00'), ('T-400', 'S-30', '12:45:00', '12:36:00'),
('T-100', 'S-99', '13:00:00', NULL), ('T-100', 'S-98', NULL, '13:15:00'),
('T-100', 'S-97', '13:30:00', '13:30:00'), ('T-100', 'S-97', '13:30:00', '13:30:00'),
('T-100', 'S-96', '06:00:00', '05:50:00'), ('T-100', 'S-95', '06:15:00', '06:10:00'),
('T-100', 'S-94', '06:30:00', '06:30:00'), ('T-300', 'S-50', '23:59:00', '23:59:59'),
('T-300', 'S-51', '24:00:00', '00:01:00'), ('T-300', 'S-52', '24:05:00', '00:06:00'),
('T-200', 'S-60', '10:00', '10:05'), ('T-200', 'S-61', '10:15 PM', '10:20 PM'),
('T-400', 'S-70', '12:00:00', '12:00:00'), ('T-400', 'S-70', '12:00:01', '12:00:01'),
('T-400', 'S-80', 'Invalid', '10:00:00'), ('T-400', 'S-80', '10:00:00', 'In Garage'),
('T-400', 'S-90', '08:00:00', '09:00:00'), ('T-400', 'S-91', '08:15:00', '09:15:00');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze bus performance in City_Transit.Bus_Telemetry.
 *  
 *  1. Bronze: Raw View of ROUTES, TRIPS, STOP_TIMES.
 *  2. Silver: 
 *     - Join: STOP_TIMES -> TRIPS -> ROUTES.
 *     - Parse Times: Convert '24:xx' timestamps to next day.
 *     - Calc Delay: Act_Arr - Sched_Arr (Seconds).
 *  3. Gold: 
 *     - Punctuality: % of stops 'On Time' (Delay between -60s and 300s).
 *  
 *  Show the SQL."
 */
