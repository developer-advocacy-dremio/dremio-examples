/*
 * Dremio "Messy Data" Challenge: Smart Home Conflicts
 * 
 * Scenario: 
 * IoT Device state log.
 * 'Conflict': Heating ON and Cooling ON at same time.
 * 'Thrashing': Device toggling On/Off > 10 times in 1 minute.
 * 
 * Objective for AI Agent:
 * 1. Detect Conflicts: Join Heating and Cooling logs on Timestamp.
 * 2. Detect Thrashing: Count state changes per minute.
 * 3. Energy Waste: Calculate duration of Conflict.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Home_Hub;
CREATE FOLDER IF NOT EXISTS Home_Hub.Devices;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Home_Hub.Devices.STATE_LOG (
    DEVICE_TYPE VARCHAR, -- 'Thermostat_Heat', 'Thermostat_Cool', 'Lights'
    TS TIMESTAMP,
    STATE VARCHAR -- 'ON', 'OFF'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Conflicts, Thrashing)
-------------------------------------------------------------------------------

-- Conflict
INSERT INTO Home_Hub.Devices.STATE_LOG VALUES
('Thermostat_Heat', '2023-01-01 10:00:00', 'ON'),
('Thermostat_Cool', '2023-01-01 10:00:00', 'ON'); -- Fighting

-- Thrashing
INSERT INTO Home_Hub.Devices.STATE_LOG VALUES
('Lights', '2023-01-01 11:00:01', 'ON'),
('Lights', '2023-01-01 11:00:02', 'OFF'),
('Lights', '2023-01-01 11:00:03', 'ON'),
('Lights', '2023-01-01 11:00:04', 'OFF'),
('Lights', '2023-01-01 11:00:05', 'ON'),
('Lights', '2023-01-01 11:00:06', 'OFF');

-- Normal
INSERT INTO Home_Hub.Devices.STATE_LOG VALUES
('Lights', '2023-01-01 12:00:00', 'ON'),
('Lights', '2023-01-01 18:00:00', 'OFF');

-- Bulk Fill
INSERT INTO Home_Hub.Devices.STATE_LOG VALUES
('DoorLock', '2023-01-01 12:00:00', 'LOCKED'),
('DoorLock', '2023-01-01 12:05:00', 'UNLOCKED'),
('DoorLock', '2023-01-01 12:05:01', 'LOCKED'),
('DoorLock', '2023-01-01 12:05:02', 'UNLOCKED'), -- Jitter
('Thermostat_Heat', '2023-01-01 13:00:00', 'OFF'),
('Thermostat_Cool', '2023-01-01 13:00:00', 'OFF'),
('Garage', '2023-01-01 14:00:00', 'OPEN'),
('Garage', '2023-01-01 23:00:00', 'OPEN'), -- Left open all night
('Sensor', '2023-01-01 00:00:00', 'Active'),
('Sensor', '2023-01-01 00:01:00', 'Inactive'),
('Sensor', NULL, 'Active'),
('Unknown', '2023-01-01 12:00:00', 'ON'),
('Light1', '2023-01-01 12:00:00', 'On'), -- Mixed case
('Light2', '2023-01-01 12:00:00', 'on'),
('Light3', '2023-01-01 12:00:00', 'TRUE'), -- Boolean string
('Light4', '2023-01-01 12:00:00', '1'), -- Int string
('Hub', '2023-01-01 12:00:00', 'Reboot'),
('Hub', '2023-01-01 12:01:00', 'Online'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'), -- Dupe
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON'),
('Thermostat_Heat', '2023-01-01 15:00:00', 'ON');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit smart home efficiency in Home_Hub.Devices.STATE_LOG.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Standardize State: Map 'On'/'TRUE'/'1' -> 'ON'.
 *  3. Gold: 
 *     - Waste Report: Time intervals where 'Thermostat_Heat'='ON' AND 'Thermostat_Cool'='ON'.
 *  
 *  Show the SQL."
 */
