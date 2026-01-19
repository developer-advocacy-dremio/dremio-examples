/*
    Dremio High-Volume SQL Pattern: Warehouse Automated Picking
    
    Business Scenario:
    A fulfillment center uses autonomous robots (AMRs) to pick goods.
    We track robot distance, battery usage, and task completion to optimize fleet efficiency.
    
    Data Story:
    - Bronze: RobotTelemetry, PickTasks.
    - Silver: FleetUtilization (Distance per charge).
    - Gold: BatteryEfficiency (Which bots act strangely?).
    
    Medallion Architecture:
    - Bronze: RobotTelemetry, PickTasks.
      *Volume*: 50+ records.
    - Silver: FleetUtilization.
    - Gold: BatteryEfficiencyAnalysis.
    
    Key Dremio Features:
    - Aggregation (SUM/AVG)
    - Math functions
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS WarehouseDB;
CREATE FOLDER IF NOT EXISTS WarehouseDB.Bronze;
CREATE FOLDER IF NOT EXISTS WarehouseDB.Silver;
CREATE FOLDER IF NOT EXISTS WarehouseDB.Gold;
USE WarehouseDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE WarehouseDB.Bronze.RobotTelemetry (
    TelemetryID INT,
    BotID STRING,
    Timestamp TIMESTAMP,
    BatteryLevel_Pct INT,
    DistanceTravelled_Meters DOUBLE,
    Status STRING -- Idle, Moving, Charging, Error
);

-- Simulating telemetry for 5 bots over a shift
INSERT INTO WarehouseDB.Bronze.RobotTelemetry VALUES
-- Bot A (Efficient)
(1, 'BOT_A', TIMESTAMP '2025-01-20 08:00:00', 100, 0.0, 'Idle'),
(2, 'BOT_A', TIMESTAMP '2025-01-20 09:00:00', 90, 500.0, 'Moving'),
(3, 'BOT_A', TIMESTAMP '2025-01-20 10:00:00', 80, 1200.0, 'Moving'),
(4, 'BOT_A', TIMESTAMP '2025-01-20 11:00:00', 70, 1800.0, 'Moving'),
(5, 'BOT_A', TIMESTAMP '2025-01-20 12:00:00', 60, 2500.0, 'Idle'),
(6, 'BOT_A', TIMESTAMP '2025-01-20 13:00:00', 90, 2500.0, 'Charging'),
(7, 'BOT_A', TIMESTAMP '2025-01-20 14:00:00', 85, 2700.0, 'Moving'),
(8, 'BOT_A', TIMESTAMP '2025-01-20 15:00:00', 75, 3500.0, 'Moving'),
(9, 'BOT_A', TIMESTAMP '2025-01-20 16:00:00', 65, 4200.0, 'Moving'),
(10, 'BOT_A', TIMESTAMP '2025-01-20 17:00:00', 55, 5000.0, 'Idle'),

-- Bot B (Battery Drain Issue)
(11, 'BOT_B', TIMESTAMP '2025-01-20 08:00:00', 100, 0.0, 'Idle'),
(12, 'BOT_B', TIMESTAMP '2025-01-20 09:00:00', 70, 400.0, 'Moving'), -- Big drop
(13, 'BOT_B', TIMESTAMP '2025-01-20 10:00:00', 40, 800.0, 'Moving'), -- Big drop
(14, 'BOT_B', TIMESTAMP '2025-01-20 11:00:00', 10, 1100.0, 'Moving'), -- Big drop
(15, 'BOT_B', TIMESTAMP '2025-01-20 12:00:00', 100, 1100.0, 'Charging'),
(16, 'BOT_B', TIMESTAMP '2025-01-20 13:00:00', 70, 1500.0, 'Moving'),
(17, 'BOT_B', TIMESTAMP '2025-01-20 14:00:00', 40, 1900.0, 'Moving'),
(18, 'BOT_B', TIMESTAMP '2025-01-20 15:00:00', 10, 2200.0, 'Moving'),
(19, 'BOT_B', TIMESTAMP '2025-01-20 16:00:00', 100, 2200.0, 'Charging'),
(20, 'BOT_B', TIMESTAMP '2025-01-20 17:00:00', 90, 2250.0, 'Idle'),

-- Bot C (Normal)
(21, 'BOT_C', TIMESTAMP '2025-01-20 08:00:00', 100, 0.0, 'Idle'),
(22, 'BOT_C', TIMESTAMP '2025-01-20 09:00:00', 92, 450.0, 'Moving'),
(23, 'BOT_C', TIMESTAMP '2025-01-20 10:00:00', 84, 900.0, 'Moving'),
(24, 'BOT_C', TIMESTAMP '2025-01-20 11:00:00', 76, 1350.0, 'Moving'),
(25, 'BOT_C', TIMESTAMP '2025-01-20 12:00:00', 68, 1800.0, 'Idle'),
(26, 'BOT_C', TIMESTAMP '2025-01-20 13:00:00', 95, 1800.0, 'Charging'),
(27, 'BOT_C', TIMESTAMP '2025-01-20 14:00:00', 87, 2250.0, 'Moving'),
(28, 'BOT_C', TIMESTAMP '2025-01-20 15:00:00', 79, 2700.0, 'Moving'),
(29, 'BOT_C', TIMESTAMP '2025-01-20 16:00:00', 71, 3150.0, 'Moving'),
(30, 'BOT_C', TIMESTAMP '2025-01-20 17:00:00', 63, 3600.0, 'Idle'),

-- Bot D (Stuck/Error)
(31, 'BOT_D', TIMESTAMP '2025-01-20 08:00:00', 100, 0.0, 'Idle'),
(32, 'BOT_D', TIMESTAMP '2025-01-20 09:00:00', 95, 200.0, 'Moving'),
(33, 'BOT_D', TIMESTAMP '2025-01-20 10:00:00', 94, 210.0, 'Error'), -- Stuck
(34, 'BOT_D', TIMESTAMP '2025-01-20 11:00:00', 93, 210.0, 'Error'),
(35, 'BOT_D', TIMESTAMP '2025-01-20 12:00:00', 92, 210.0, 'Error'),
(36, 'BOT_D', TIMESTAMP '2025-01-20 13:00:00', 91, 210.0, 'Idle'), -- Fixed
(37, 'BOT_D', TIMESTAMP '2025-01-20 14:00:00', 85, 500.0, 'Moving'),
(38, 'BOT_D', TIMESTAMP '2025-01-20 15:00:00', 78, 800.0, 'Moving'),
(39, 'BOT_D', TIMESTAMP '2025-01-20 16:00:00', 70, 1100.0, 'Moving'),
(40, 'BOT_D', TIMESTAMP '2025-01-20 17:00:00', 65, 1300.0, 'Idle'),

-- Bot E (High Mileage)
(41, 'BOT_E', TIMESTAMP '2025-01-20 08:00:00', 100, 0.0, 'Idle'),
(42, 'BOT_E', TIMESTAMP '2025-01-20 09:00:00', 88, 600.0, 'Moving'),
(43, 'BOT_E', TIMESTAMP '2025-01-20 10:00:00', 76, 1400.0, 'Moving'),
(44, 'BOT_E', TIMESTAMP '2025-01-20 11:00:00', 64, 2200.0, 'Moving'),
(45, 'BOT_E', TIMESTAMP '2025-01-20 12:00:00', 52, 3000.0, 'Moving'),
(46, 'BOT_E', TIMESTAMP '2025-01-20 13:00:00', 95, 3000.0, 'Charging'),
(47, 'BOT_E', TIMESTAMP '2025-01-20 14:00:00', 80, 3800.0, 'Moving'),
(48, 'BOT_E', TIMESTAMP '2025-01-20 15:00:00', 65, 4800.0, 'Moving'),
(49, 'BOT_E', TIMESTAMP '2025-01-20 16:00:00', 50, 5800.0, 'Moving'),
(50, 'BOT_E', TIMESTAMP '2025-01-20 17:00:00', 35, 6600.0, 'Idle');

CREATE OR REPLACE TABLE WarehouseDB.Bronze.PickTasks (
    TaskID STRING,
    BotID STRING,
    SKU STRING,
    Location STRING,
    TaskTime_Secs INT
);

INSERT INTO WarehouseDB.Bronze.PickTasks VALUES
('T001', 'BOT_A', 'SKU-001', 'A1-01', 45),
('T002', 'BOT_A', 'SKU-002', 'A1-02', 40),
('T003', 'BOT_B', 'SKU-003', 'B1-01', 50),
('T004', 'BOT_B', 'SKU-004', 'B1-02', 48),
('T005', 'BOT_C', 'SKU-005', 'C1-01', 42),
('T006', 'BOT_C', 'SKU-006', 'C1-02', 39),
('T007', 'BOT_D', 'SKU-007', 'D1-01', 200), -- Long due to error
('T008', 'BOT_E', 'SKU-008', 'E1-01', 35),
('T009', 'BOT_E', 'SKU-009', 'E1-02', 30),
('T010', 'BOT_A', 'SKU-010', 'A2-01', 44),
('T011', 'BOT_B', 'SKU-011', 'B2-01', 52),
('T012', 'BOT_C', 'SKU-012', 'C2-01', 41),
('T013', 'BOT_A', 'SKU-013', 'A2-02', 46),
('T014', 'BOT_B', 'SKU-014', 'B2-02', 49),
('T015', 'BOT_E', 'SKU-015', 'E2-01', 33),
('T016', 'BOT_A', 'SKU-016', 'A3-01', 47),
('T017', 'BOT_B', 'SKU-017', 'B3-01', 55),
('T018', 'BOT_C', 'SKU-018', 'C3-01', 40),
('T019', 'BOT_D', 'SKU-019', 'D3-01', 180),
('T020', 'BOT_E', 'SKU-020', 'E3-01', 32),
('T021', 'BOT_A', 'SKU-021', 'A4-01', 45),
('T022', 'BOT_B', 'SKU-022', 'B4-01', 51),
('T023', 'BOT_C', 'SKU-023', 'C4-01', 43),
('T024', 'BOT_A', 'SKU-024', 'A5-01', 44),
('T025', 'BOT_B', 'SKU-025', 'B5-01', 53),
('T026', 'BOT_E', 'SKU-026', 'E4-01', 34),
('T027', 'BOT_A', 'SKU-027', 'A6-01', 46),
('T028', 'BOT_B', 'SKU-028', 'B6-01', 54),
('T029', 'BOT_C', 'SKU-029', 'C5-01', 42),
('T030', 'BOT_E', 'SKU-030', 'E5-01', 31),
('T031', 'BOT_A', 'SKU-031', 'A7-01', 45),
('T032', 'BOT_B', 'SKU-032', 'B7-01', 50),
('T033', 'BOT_C', 'SKU-033', 'C6-01', 41),
('T034', 'BOT_D', 'SKU-034', 'D4-01', 60), -- Recovered
('T035', 'BOT_E', 'SKU-035', 'E6-01', 30),
('T036', 'BOT_A', 'SKU-036', 'A8-01', 44),
('T037', 'BOT_B', 'SKU-037', 'B8-01', 52),
('T038', 'BOT_C', 'SKU-038', 'C7-01', 40),
('T039', 'BOT_A', 'SKU-039', 'A9-01', 46),
('T040', 'BOT_B', 'SKU-040', 'B9-01', 51),
('T041', 'BOT_E', 'SKU-041', 'E7-01', 29),
('T042', 'BOT_A', 'SKU-042', 'A10-01', 47),
('T043', 'BOT_B', 'SKU-043', 'B10-01', 53),
('T044', 'BOT_C', 'SKU-044', 'C8-01', 42),
('T045', 'BOT_A', 'SKU-045', 'A11-01', 45),
('T046', 'BOT_B', 'SKU-046', 'B11-01', 50),
('T047', 'BOT_C', 'SKU-047', 'C9-01', 41),
('T048', 'BOT_D', 'SKU-048', 'D5-01', 58),
('T049', 'BOT_E', 'SKU-049', 'E8-01', 28),
('T050', 'BOT_A', 'SKU-050', 'A12-01', 44);


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Fleet Utilization
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW WarehouseDB.Silver.FleetUtilization AS
SELECT
    BotID,
    MAX(DistanceTravelled_Meters) AS MaxDistance,
    MIN(BatteryLevel_Pct) AS MinBattery,
    COUNT(*) AS TelemetryPings
FROM WarehouseDB.Bronze.RobotTelemetry
GROUP BY BotID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Battery Efficiency Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW WarehouseDB.Gold.BatteryEfficiency AS
SELECT
    BotID,
    MaxDistance,
    (100 - MinBattery) AS BatteryConsumed_Pct,
    CASE 
        WHEN (100 - MinBattery) > 0 THEN MaxDistance / (100 - MinBattery)
        ELSE 0 
    END AS MetersPerPctCharge
FROM WarehouseDB.Silver.FleetUtilization;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Identify the most energy-efficient robot (Meters per % Charge)."
    2. "List robots that travelled more than 5000 meters."
    3. "Which robot had the biggest battery drain?"
*/
