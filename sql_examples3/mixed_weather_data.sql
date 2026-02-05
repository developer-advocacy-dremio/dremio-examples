/*
 * Dremio "Messy Data" Challenge: Mixed Weather Station Data
 * 
 * Scenario: 
 * Sensor Network uses different firmware versions.
 * 'V1' Sensors report in Fahrenheit. 'V2' Sensors report in Celsius.
 * Broken sensors report '9999' or '-9999'.
 * 'Wind_Speed' is sometimes knots, sometimes mph, sometimes km/h (indicated by SENSOR_TYPE).
 * 
 * Objective for AI Agent:
 * 1. Normalize Temperature to Celsius: (F - 32) * 5/9 if Type=V1.
 * 2. Filter out error codes (Abs(Temp) > 100).
 * 3. Normalize Wind Speed to km/h.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Weather_Grid;
CREATE FOLDER IF NOT EXISTS Weather_Grid.Sensor_Logs;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Weather_Grid.Sensor_Logs.OBSERVATIONS (
    SENSOR_ID VARCHAR,
    SENSOR_TYPE VARCHAR, -- 'V1_USA', 'V2_EU', 'V3_MAR'
    TS TIMESTAMP,
    TEMP_VAL DOUBLE,
    WIND_VAL DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Unit Mixing)
-------------------------------------------------------------------------------

-- V1 (Fahrenheit, MPH)
INSERT INTO Weather_Grid.Sensor_Logs.OBSERVATIONS VALUES
('S-100', 'V1_USA', '2023-01-01 12:00:00', 32.0, 10.0), -- 0C
('S-101', 'V1_USA', '2023-01-01 12:00:00', 212.0, 0.0); -- 100C

-- V2 (Celsius, KMH)
INSERT INTO Weather_Grid.Sensor_Logs.OBSERVATIONS VALUES
('S-200', 'V2_EU', '2023-01-01 12:00:00', 0.0, 16.0), -- Matches 32F
('S-201', 'V2_EU', '2023-01-01 12:00:00', 100.0, 0.0); -- Matches 212F

-- V3 (Marine - Celsius, Knots)
INSERT INTO Weather_Grid.Sensor_Logs.OBSERVATIONS VALUES
('S-300', 'V3_MAR', '2023-01-01 12:00:00', 20.0, 10.0); -- 10 knots != 10 kmh

-- Errors
INSERT INTO Weather_Grid.Sensor_Logs.OBSERVATIONS VALUES
('S-ERR', 'V1_USA', '2023-01-01 12:00:00', 9999.0, -1.0),
('S-ERR', 'V2_EU', '2023-01-01 12:05:00', -9999.0, 999.0);

-- Bulk Fill
INSERT INTO Weather_Grid.Sensor_Logs.OBSERVATIONS VALUES
('S-102', 'V1_USA', '2023-01-01 13:00:00', 70.0, 5.0),
('S-103', 'V1_USA', '2023-01-01 13:00:00', 75.0, 6.0),
('S-104', 'V1_USA', '2023-01-01 13:00:00', 80.0, 7.0),
('S-105', 'V1_USA', '2023-01-01 13:00:00', 85.0, 8.0),
('S-106', 'V1_USA', '2023-01-01 13:00:00', 90.0, 9.0),
('S-202', 'V2_EU', '2023-01-01 13:00:00', 21.0, 10.0),
('S-203', 'V2_EU', '2023-01-01 13:00:00', 22.0, 12.0),
('S-204', 'V2_EU', '2023-01-01 13:00:00', 23.0, 14.0),
('S-205', 'V2_EU', '2023-01-01 13:00:00', 24.0, 16.0),
('S-206', 'V2_EU', '2023-01-01 13:00:00', 25.0, 18.0),
('S-301', 'V3_MAR', '2023-01-01 13:00:00', 15.0, 20.0),
('S-302', 'V3_MAR', '2023-01-01 13:00:00', 16.0, 22.0),
('S-303', 'V3_MAR', '2023-01-01 13:00:00', 17.0, 24.0),
('S-304', 'V3_MAR', '2023-01-01 13:00:00', 18.0, 26.0),
('S-305', 'V3_MAR', '2023-01-01 13:00:00', 19.0, 28.0),
('S-107', 'V1_USA', '2023-01-01 14:00:00', 30.0, 2.0), -- Below freezing
('S-108', 'V1_USA', '2023-01-01 14:00:00', 10.0, 5.0),
('S-109', 'V1_USA', '2023-01-01 14:00:00', -10.0, 10.0), -- Negative F
('S-207', 'V2_EU', '2023-01-01 14:00:00', -1.0, 5.0), -- Negative C
('S-208', 'V2_EU', '2023-01-01 14:00:00', -5.0, 10.0),
('S-209', 'V2_EU', '2023-01-01 14:00:00', -10.0, 15.0),
('S-306', 'V3_MAR', '2023-01-01 14:00:00', 25.0, 50.0), -- High wind
('S-307', 'V3_MAR', '2023-01-01 14:00:00', 25.0, 60.0),
('S-308', 'V3_MAR', '2023-01-01 14:00:00', 25.0, 70.0),
('S-Err2', 'V1_USA', '2023-01-01 15:00:00', 9999.0, 0.0),
('S-Err3', 'V1_USA', '2023-01-01 15:00:00', 9999.0, 0.0),
('S-Err4', 'V1_USA', '2023-01-01 15:00:00', 9999.0, 0.0),
('S-Err5', 'V2_EU', '2023-01-01 15:00:00', -9999.0, 0.0),
('S-Err6', 'V2_EU', '2023-01-01 15:00:00', -9999.0, 0.0),
('S-400', 'V1_USA', '2023-01-01 16:00:00', 60.0, 5.0),
('S-401', 'V1_USA', '2023-01-01 16:00:00', 60.0, 5.0), -- Dupe
('S-402', 'V1_USA', '2023-01-01 16:00:00', 60.0, 5.0), -- Tripe
('S-403', 'V1_USA', '2023-01-01 16:00:00', NULL, NULL), -- Nulls
('S-404', 'V2_EU', '2023-01-01 16:00:00', NULL, 10.0),
('S-405', 'V3_MAR', '2023-01-01 16:00:00', 20.0, NULL),
('S-500', 'Unknown', '2023-01-01 17:00:00', 50.0, 50.0), -- Unknown type
('S-501', 'Unknown', '2023-01-01 17:05:00', 50.0, 50.0),
('S-502', 'Unknown', '2023-01-01 17:10:00', 50.0, 50.0),
('S-503', 'Unknown', '2023-01-01 17:15:00', 50.0, 50.0);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Normalize the weather data in Weather_Grid.Sensor_Logs.OBSERVATIONS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Normalize Temp to Celsius:
 *       - If SENSOR_TYPE like '%USA', (TEMP_VAL - 32) * 5/9.
 *       - Else TEMP_VAL.
 *     - Normalize Wind to KMH:
 *       - If SENSOR_TYPE like '%MAR', WIND_VAL * 1.852 (Knots to KMH).
 *       - If SENSOR_TYPE like '%USA', WIND_VAL * 1.609 (MPH to KMH).
 *     - Filter out outliers (> 100C or < -100C).
 *  3. Gold: 
 *     - Avg Temp (C) and Max Wind (KMH) per Sensor Type.
 *  
 *  Show the SQL."
 */
