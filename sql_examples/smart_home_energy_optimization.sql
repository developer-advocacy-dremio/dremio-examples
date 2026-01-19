/*
 * Smart Home Energy Optimization Demo
 * 
 * Scenario:
 * A smart home platform analyzes device usage patterns to identify "energy vampires,"
 * optimize thermostat settings, and reduce electricity bills.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Devices: Registry of smart plugs, thermostats, and appliances.
 * - Energy_Usage: High-frequency power draw logs (Watts).
 * - User_Preferences: Target temperatures and budget settings.
 * 
 * Silver Layer:
 * - Daily_Consumption: Aggregated KWh per device per day.
 * - Idle_Detection: Flagging devices drawing power when 'Off' or inactive.
 * 
 * Gold Layer:
 * - Cost_Savings_Opportunities: Recommendations to unplug or schedule devices.
 * - Energy_Vampire_Rank: Ranking devices by wasted standby power.
 * 
 * Note: Assumes a catalog named 'HomeEnergyDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HomeEnergyDB;
CREATE FOLDER IF NOT EXISTS HomeEnergyDB.Bronze;
CREATE FOLDER IF NOT EXISTS HomeEnergyDB.Silver;
CREATE FOLDER IF NOT EXISTS HomeEnergyDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS HomeEnergyDB.Bronze.Devices (
    DeviceID INT,
    DeviceName VARCHAR,
    Category VARCHAR, -- HVAC, Lighting, Entertainment, Appliance
    Room VARCHAR,
    IsSmartPlug BOOLEAN
);

CREATE TABLE IF NOT EXISTS HomeEnergyDB.Bronze.Energy_Usage (
    LogID INT,
    DeviceID INT,
    Timestamp TIMESTAMP,
    PowerWatts DOUBLE,
    Status VARCHAR -- On, Off, Standby
);

CREATE TABLE IF NOT EXISTS HomeEnergyDB.Bronze.User_Preferences (
    UserID INT,
    PreferedTempC DOUBLE,
    BudgetLimitUSD DOUBLE,
    EcoMode BOOLEAN
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Devices
INSERT INTO HomeEnergyDB.Bronze.Devices (DeviceID, DeviceName, Category, Room, IsSmartPlug) VALUES
(1, 'Living Room TV', 'Entertainment', 'Living Room', true),
(2, 'Game Console', 'Entertainment', 'Living Room', true),
(3, 'Kitchen Fridge', 'Appliance', 'Kitchen', false),
(4, 'Main Thermostat', 'HVAC', 'Hallway', true),
(5, 'Bedroom Lamp', 'Lighting', 'Bedroom', true),
(6, 'Office Monitor', 'Entertainment', 'Office', true),
(7, 'Coffee Maker', 'Appliance', 'Kitchen', true),
(8, 'Washing Machine', 'Appliance', 'Laundry', false),
(9, 'Garage Heater', 'HVAC', 'Garage', true),
(10, 'Sound Bar', 'Entertainment', 'Living Room', true);

-- Insert User Prefs
INSERT INTO HomeEnergyDB.Bronze.User_Preferences (UserID, PreferedTempC, BudgetLimitUSD, EcoMode) VALUES
(1, 21.5, 150.0, true);

-- Insert 50 records into HomeEnergyDB.Bronze.Energy_Usage
-- Simulating vampire power for TV/Console, efficient Lamp, cyclic Fridge
INSERT INTO HomeEnergyDB.Bronze.Energy_Usage (LogID, DeviceID, Timestamp, PowerWatts, Status) VALUES
(1, 1, '2025-01-01 08:00:00', 15.0, 'Standby'), -- Vampire draw
(2, 1, '2025-01-01 09:00:00', 15.0, 'Standby'),
(3, 1, '2025-01-01 19:00:00', 150.0, 'On'),
(4, 1, '2025-01-01 20:00:00', 145.0, 'On'),
(5, 1, '2025-01-01 22:00:00', 15.0, 'Standby'),
(6, 2, '2025-01-01 08:00:00', 10.0, 'Standby'),
(7, 2, '2025-01-01 20:00:00', 200.0, 'On'), -- Gaming
(8, 2, '2025-01-01 21:00:00', 10.0, 'Standby'), -- Left plugged in
(9, 3, '2025-01-01 08:00:00', 100.0, 'On'), -- Compressor cycle
(10, 3, '2025-01-01 09:00:00', 10.0, 'Standby'),
(11, 3, '2025-01-01 10:00:00', 100.0, 'On'),
(12, 4, '2025-01-01 08:00:00', 3500.0, 'On'), -- Heating
(13, 4, '2025-01-01 09:00:00', 5.0, 'Standby'), -- Idle
(14, 5, '2025-01-01 22:00:00', 12.0, 'On'),
(15, 5, '2025-01-01 23:00:00', 0.0, 'Off'),
(16, 6, '2025-01-01 09:00:00', 35.0, 'On'),
(17, 6, '2025-01-01 17:00:00', 2.0, 'Standby'),
(18, 7, '2025-01-01 07:00:00', 800.0, 'On'),
(19, 7, '2025-01-01 08:00:00', 5.0, 'Standby'),
(20, 8, '2025-01-01 10:00:00', 500.0, 'On'),
(21, 8, '2025-01-01 11:00:00', 0.5, 'Standby'),
(22, 9, '2025-01-01 05:00:00', 2000.0, 'On'),
(23, 9, '2025-01-01 08:00:00', 0.0, 'Off'),
(24, 10, '2025-01-01 19:00:00', 50.0, 'On'),
(25, 10, '2025-01-01 23:00:00', 8.0, 'Standby'), -- Vampire
(26, 1, '2025-01-02 08:00:00', 15.0, 'Standby'),
(27, 2, '2025-01-02 08:00:00', 10.0, 'Standby'),
(28, 6, '2025-01-02 09:00:00', 35.0, 'On'),
(29, 6, '2025-01-02 18:00:00', 2.0, 'Standby'),
(30, 7, '2025-01-02 07:15:00', 800.0, 'On'),
(31, 10, '2025-01-02 07:00:00', 8.0, 'Standby'),
(32, 4, '2025-01-02 06:00:00', 3500.0, 'On'),
(33, 3, '2025-01-02 12:00:00', 100.0, 'On'),
(34, 1, '2025-01-03 08:00:00', 15.0, 'Standby'),
(35, 1, '2025-01-03 18:00:00', 150.0, 'On'),
(36, 10, '2025-01-03 18:00:00', 50.0, 'On'),
(37, 2, '2025-01-03 20:00:00', 200.0, 'On'),
(38, 2, '2025-01-03 23:00:00', 10.0, 'Standby'),
(39, 4, '2025-01-03 07:00:00', 3500.0, 'On'),
(40, 5, '2025-01-03 21:00:00', 12.0, 'On'),
(41, 5, '2025-01-03 23:30:00', 0.0, 'Off'),
(42, 7, '2025-01-03 07:00:00', 800.0, 'On'),
(43, 8, '2025-01-03 14:00:00', 500.0, 'On'),
(44, 9, '2025-01-03 22:00:00', 2000.0, 'On'),
(45, 1, '2025-01-04 01:00:00', 15.0, 'Standby'),
(46, 2, '2025-01-04 01:00:00', 10.0, 'Standby'),
(47, 10, '2025-01-04 01:00:00', 8.0, 'Standby'),
(48, 4, '2025-01-04 05:00:00', 3500.0, 'On'),
(49, 3, '2025-01-04 06:00:00', 100.0, 'On'),
(50, 6, '2025-01-04 10:00:00', 35.0, 'On');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Usage Analysis
-------------------------------------------------------------------------------

-- 2.1 Daily Consumption
-- Approximated integration: PowerWatts * 1 hour (as samples are sparse) / 1000 = KWh
CREATE OR REPLACE VIEW HomeEnergyDB.Silver.Daily_Device_Consumption AS
SELECT
    e.DeviceID,
    d.DeviceName,
    d.Category,
    TO_DATE(e.Timestamp) AS UsageDate,
    SUM(e.PowerWatts) / 1000.0 AS EstimatedKWh,
    SUM(CASE WHEN e.Status = 'Standby' THEN e.PowerWatts ELSE 0 END) / 1000.0 AS WastedStandbyKWh
FROM HomeEnergyDB.Bronze.Energy_Usage e
JOIN HomeEnergyDB.Bronze.Devices d ON e.DeviceID = d.DeviceID
GROUP BY e.DeviceID, d.DeviceName, d.Category, TO_DATE(e.Timestamp);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Optimizations
-------------------------------------------------------------------------------

-- 3.1 Energy Vampire Rank
-- Devices that consume the most power while 'Standby'
CREATE OR REPLACE VIEW HomeEnergyDB.Gold.Energy_Vampire_Rank AS
SELECT
    DeviceName,
    SUM(WastedStandbyKWh) AS TotalWastedKWh,
    SUM(WastedStandbyKWh) * 0.15 AS EstimatedDailyCostUSD -- assuming $0.15/kWh
FROM HomeEnergyDB.Silver.Daily_Device_Consumption
GROUP BY DeviceName
ORDER BY TotalWastedKWh DESC;

-- 3.2 Savings Opportunities
CREATE OR REPLACE VIEW HomeEnergyDB.Gold.Cost_Savings_Opportunities AS
SELECT
    DeviceName,
    'High Standby Usage' AS Issue,
    'Unplug or use Smart Strip' AS Action
FROM HomeEnergyDB.Gold.Energy_Vampire_Rank
WHERE TotalWastedKWh > 0.05;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Identify Waste):
"Which device is the top energy vampire in HomeEnergyDB.Gold.Energy_Vampire_Rank?"

PROMPT 2 (Daily Usage):
"Show the EstimatedKWh usage for 'HVAC' devices on '2025-01-01' from HomeEnergyDB.Silver.Daily_Device_Consumption."

PROMPT 3 (Budget Check):
"List all recommendations from HomeEnergyDB.Gold.Cost_Savings_Opportunities."
*/
