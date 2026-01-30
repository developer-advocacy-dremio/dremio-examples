/*
 * Events: Music Festival Logistics
 * 
 * Scenario:
 * Managing stage crowd density and real-time water station supply levels to prevent overcrowding.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FestivalDB;
CREATE FOLDER IF NOT EXISTS FestivalDB.Logistics;
CREATE FOLDER IF NOT EXISTS FestivalDB.Logistics.Bronze;
CREATE FOLDER IF NOT EXISTS FestivalDB.Logistics.Silver;
CREATE FOLDER IF NOT EXISTS FestivalDB.Logistics.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Sensor Data
-------------------------------------------------------------------------------

-- StageSensors Table (Crowd Density)
CREATE TABLE IF NOT EXISTS FestivalDB.Logistics.Bronze.StageSensors (
    SensorID VARCHAR,
    StageName VARCHAR,
    MaxCapacity INT,
    CurrentHeadCount INT,
    Timestamp TIMESTAMP
);

INSERT INTO FestivalDB.Logistics.Bronze.StageSensors VALUES
('S-01', 'Main Stage', 50000, 45000, '2025-07-15 20:00:00'),
('S-02', 'Main Stage', 50000, 46000, '2025-07-15 20:15:00'),
('S-03', 'Main Stage', 50000, 48000, '2025-07-15 20:30:00'), -- High Density
('S-04', 'Techno Tent', 10000, 2000, '2025-07-15 20:00:00'),
('S-05', 'Techno Tent', 10000, 2500, '2025-07-15 20:15:00'),
('S-06', 'Techno Tent', 10000, 5000, '2025-07-15 20:30:00'),
('S-07', 'Indie Stage', 5000, 5100, '2025-07-15 20:00:00'), -- Over Capacity
('S-08', 'Indie Stage', 5000, 5200, '2025-07-15 20:15:00'), -- Over Capacity
('S-09', 'Indie Stage', 5000, 4000, '2025-07-15 20:30:00'),
('S-10', 'Main Stage', 50000, 42000, '2025-07-15 21:00:00'),
('S-11', 'Techno Tent', 10000, 8000, '2025-07-15 21:00:00'),
('S-12', 'Indie Stage', 5000, 3000, '2025-07-15 21:00:00'),
('S-13', 'Silent Disco', 2000, 1500, '2025-07-15 20:00:00'),
('S-14', 'Silent Disco', 2000, 1800, '2025-07-15 20:15:00'),
('S-15', 'Silent Disco', 2000, 1900, '2025-07-15 20:30:00'),
('S-16', 'Comedy Tent', 1000, 800, '2025-07-15 20:00:00'),
('S-17', 'Comedy Tent', 1000, 900, '2025-07-15 20:15:00'),
('S-18', 'Comedy Tent', 1000, 950, '2025-07-15 20:30:00'), -- Near Cap
('S-19', 'Chill Zone', 5000, 1000, '2025-07-15 20:00:00'),
('S-20', 'Chill Zone', 5000, 1200, '2025-07-15 20:15:00'),
('S-21', 'Chill Zone', 5000, 1500, '2025-07-15 20:30:00'),
('S-22', 'VIP Deck', 1000, 500, '2025-07-15 20:00:00'),
('S-23', 'VIP Deck', 1000, 600, '2025-07-15 20:15:00'),
('S-24', 'VIP Deck', 1000, 800, '2025-07-15 20:30:00'),
('S-25', 'Main Stage', 50000, 49000, '2025-07-15 20:45:00'), -- Critical
('S-26', 'Main Stage', 50000, 49500, '2025-07-15 20:50:00'), -- Critical
('S-27', 'Indie Stage', 5000, 4500, '2025-07-15 20:45:00'),
('S-28', 'Techno Tent', 10000, 9000, '2025-07-15 20:45:00'),
('S-29', 'Silent Disco', 2000, 1950, '2025-07-15 20:45:00'), -- Near Cap
('S-30', 'Comedy Tent', 1000, 1000, '2025-07-15 20:45:00'), -- Full
('S-31', 'Main Stage', 50000, 45000, '2025-07-16 14:00:00'),
('S-32', 'Techno Tent', 10000, 2000, '2025-07-16 14:00:00'),
('S-33', 'Indie Stage', 5000, 2500, '2025-07-16 14:00:00'),
('S-34', 'Silent Disco', 2000, 500, '2025-07-16 14:00:00'),
('S-35', 'Comedy Tent', 1000, 300, '2025-07-16 14:00:00'),
('S-36', 'Main Stage', 50000, 46000, '2025-07-16 15:00:00'),
('S-37', 'Techno Tent', 10000, 3000, '2025-07-16 15:00:00'),
('S-38', 'Indie Stage', 5000, 3000, '2025-07-16 15:00:00'),
('S-39', 'Silent Disco', 2000, 800, '2025-07-16 15:00:00'),
('S-40', 'Comedy Tent', 1000, 400, '2025-07-16 15:00:00'),
('S-41', 'Main Stage', 50000, 47000, '2025-07-16 16:00:00'),
('S-42', 'Techno Tent', 10000, 4000, '2025-07-16 16:00:00'),
('S-43', 'Indie Stage', 5000, 3500, '2025-07-16 16:00:00'),
('S-44', 'Silent Disco', 2000, 1000, '2025-07-16 16:00:00'),
('S-45', 'Comedy Tent', 1000, 500, '2025-07-16 16:00:00'),
('S-46', 'Main Stage', 50000, 48000, '2025-07-16 17:00:00'),
('S-47', 'Techno Tent', 10000, 5000, '2025-07-16 17:00:00'),
('S-48', 'Indie Stage', 5000, 4000, '2025-07-16 17:00:00'),
('S-49', 'Silent Disco', 2000, 1200, '2025-07-16 17:00:00'),
('S-50', 'Comedy Tent', 1000, 600, '2025-07-16 17:00:00');

-- SupplyInventory Table (Water Stations)
CREATE TABLE IF NOT EXISTS FestivalDB.Logistics.Bronze.SupplyInventory (
    StationID VARCHAR,
    Location VARCHAR,
    Item VARCHAR, -- WaterBottles
    CurrentStock INT,
    MaxStock INT,
    LastRestock TIMESTAMP
);

INSERT INTO FestivalDB.Logistics.Bronze.SupplyInventory VALUES
('W-01', 'Main Stage Left', 'WaterBottles', 500, 5000, '2025-07-15 19:00:00'), -- Low
('W-02', 'Main Stage Right', 'WaterBottles', 4500, 5000, '2025-07-15 20:00:00'),
('W-03', 'Techno Tent', 'WaterBottles', 200, 2000, '2025-07-15 18:00:00'), -- Low
('W-04', 'Indie Stage', 'WaterBottles', 1500, 2000, '2025-07-15 19:30:00'),
('W-05', 'Entrance', 'WaterBottles', 8000, 10000, '2025-07-15 12:00:00'),
('W-06', 'VIP Area', 'WaterBottles', 5000, 5000, '2025-07-15 20:00:00'),
('W-07', 'Camping A', 'WaterBottles', 200, 5000, '2025-07-15 10:00:00'), -- Critical
('W-08', 'Camping B', 'WaterBottles', 3000, 5000, '2025-07-15 12:00:00'),
('W-09', 'Silent Disco', 'WaterBottles', 800, 1000, '2025-07-15 19:00:00'),
('W-10', 'Food Court', 'WaterBottles', 4000, 8000, '2025-07-15 18:00:00'),
('W-11', 'Chill Zone', 'WaterBottles', 100, 2000, '2025-07-15 15:00:00'), -- Critical
('W-12', 'Medical Tent', 'WaterBottles', 1000, 1000, '2025-07-15 20:00:00'),
('W-13', 'Backstage Main', 'WaterBottles', 2000, 2000, '2025-07-15 20:00:00'),
('W-14', 'Backstage Indie', 'WaterBottles', 500, 1000, '2025-07-15 18:00:00'),
('W-15', 'Parking Lot', 'WaterBottles', 200, 2000, '2025-07-15 12:00:00'), -- Low
('W-16', 'Shuttle Stop', 'WaterBottles', 300, 1000, '2025-07-15 14:00:00'),
('W-17', 'Main Stage Left', 'WaterBottles', 4000, 5000, '2025-07-16 12:00:00'), -- Restocked
('W-18', 'Main Stage Right', 'WaterBottles', 4200, 5000, '2025-07-16 12:00:00'),
('W-19', 'Techno Tent', 'WaterBottles', 1800, 2000, '2025-07-16 12:00:00'),
('W-20', 'Indie Stage', 'WaterBottles', 1900, 2000, '2025-07-16 12:00:00'),
('W-21', 'Entrance', 'WaterBottles', 9000, 10000, '2025-07-16 12:00:00'),
('W-22', 'VIP Area', 'WaterBottles', 4800, 5000, '2025-07-16 12:00:00'),
('W-23', 'Camping A', 'WaterBottles', 4500, 5000, '2025-07-16 08:00:00'),
('W-24', 'Camping B', 'WaterBottles', 4600, 5000, '2025-07-16 08:00:00'),
('W-25', 'Silent Disco', 'WaterBottles', 900, 1000, '2025-07-16 14:00:00'),
('W-26', 'Food Court', 'WaterBottles', 7500, 8000, '2025-07-16 12:00:00'),
('W-27', 'Chill Zone', 'WaterBottles', 1900, 2000, '2025-07-16 12:00:00'),
('W-28', 'Medical Tent', 'WaterBottles', 950, 1000, '2025-07-16 12:00:00'),
('W-29', 'Backstage Main', 'WaterBottles', 1900, 2000, '2025-07-16 12:00:00'),
('W-30', 'Backstage Indie', 'WaterBottles', 900, 1000, '2025-07-16 12:00:00'),
('W-31', 'Parking Lot', 'WaterBottles', 1500, 2000, '2025-07-16 12:00:00'),
('W-32', 'Shuttle Stop', 'WaterBottles', 900, 1000, '2025-07-16 12:00:00'),
('W-33', 'Main Stage Left', 'WaterBottles', 3000, 5000, '2025-07-16 16:00:00'),
('W-34', 'Main Stage Right', 'WaterBottles', 3500, 5000, '2025-07-16 16:00:00'),
('W-35', 'Techno Tent', 'WaterBottles', 1500, 2000, '2025-07-16 16:00:00'),
('W-36', 'Indie Stage', 'WaterBottles', 1600, 2000, '2025-07-16 16:00:00'),
('W-37', 'Entrance', 'WaterBottles', 8000, 10000, '2025-07-16 16:00:00'),
('W-38', 'VIP Area', 'WaterBottles', 4500, 5000, '2025-07-16 16:00:00'),
('W-39', 'Camping A', 'WaterBottles', 4000, 5000, '2025-07-16 16:00:00'),
('W-40', 'Camping B', 'WaterBottles', 4200, 5000, '2025-07-16 16:00:00'),
('W-41', 'Silent Disco', 'WaterBottles', 800, 1000, '2025-07-16 16:00:00'),
('W-42', 'Food Court', 'WaterBottles', 7000, 8000, '2025-07-16 16:00:00'),
('W-43', 'Chill Zone', 'WaterBottles', 1500, 2000, '2025-07-16 16:00:00'),
('W-44', 'Medical Tent', 'WaterBottles', 900, 1000, '2025-07-16 16:00:00'),
('W-45', 'Backstage Main', 'WaterBottles', 1800, 2000, '2025-07-16 16:00:00'),
('W-46', 'Backstage Indie', 'WaterBottles', 850, 1000, '2025-07-16 16:00:00'),
('W-47', 'Parking Lot', 'WaterBottles', 1000, 2000, '2025-07-16 16:00:00'),
('W-48', 'Shuttle Stop', 'WaterBottles', 800, 1000, '2025-07-16 16:00:00'),
('W-49', 'Entrance', 'WaterBottles', 100, 10000, '2025-07-15 08:00:00'), -- Empty start? Risk.
('W-50', 'Camping A', 'WaterBottles', 100, 5000, '2025-07-15 08:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Capacity Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FestivalDB.Logistics.Silver.StageDensity AS
SELECT 
    StageName,
    MaxCapacity,
    CurrentHeadCount,
    (CAST(CurrentHeadCount AS DOUBLE) / MaxCapacity) * 100 AS CapacityUtilizationPct,
    Timestamp
FROM FestivalDB.Logistics.Bronze.StageSensors;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Safety Alerts
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FestivalDB.Logistics.Gold.SafetyAlerts AS
SELECT 
    StageName,
    'Crowd Crush Risk' AS AlertType,
    CapacityUtilizationPct,
    Timestamp 
FROM FestivalDB.Logistics.Silver.StageDensity
WHERE CapacityUtilizationPct > 95.0
UNION ALL
SELECT 
    Location AS StageName,
    'Low Water Supply' AS AlertType,
    (CAST(CurrentStock AS DOUBLE) / MaxStock) * 100 AS Metric,
    LastRestock AS Timestamp
FROM FestivalDB.Logistics.Bronze.SupplyInventory
WHERE (CAST(CurrentStock AS DOUBLE) / MaxStock) * 100 < 10.0;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Find all 'Crowd Crush Risk' alerts in the FestivalDB.Logistics.Gold.SafetyAlerts view sorted by timestamp."

PROMPT 2:
"Identify which water stations have less than 10% stock remaining from the Bronze layer."

PROMPT 3:
"Calculate average capacity utilization for the 'Main Stage' across all time periods."
*/
