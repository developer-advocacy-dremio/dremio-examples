/*
 * Manufacturing Predictive Maintenance Demo
 * 
 * Scenario:
 * A factory monitors vibration and temperature sensors to predict machine failures.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Reduce downtime and optimize maintenance schedules.
 * 
 * Note: Assumes a catalog named 'ManufacturingDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ManufacturingDB.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ManufacturingDB.Bronze.Machines (
    MachineID INT,
    Model VARCHAR,
    InstallDate DATE,
    Location VARCHAR
);

CREATE TABLE IF NOT EXISTS ManufacturingDB.Bronze.SensorReadings (
    ReadingID INT,
    MachineID INT,
    "Timestamp" TIMESTAMP,
    VibrationLevel DOUBLE,
    Temperature DOUBLE
);

CREATE TABLE IF NOT EXISTS ManufacturingDB.Bronze.MaintenanceLogs (
    LogID INT,
    MachineID INT,
    MaintenanceDate DATE,
    Type VARCHAR, -- 'Routine', 'Repair', 'Replacement'
    Cost DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO ManufacturingDB.Bronze.Machines (MachineID, Model, InstallDate, Location) VALUES
(1, 'Press-X100', '2020-01-15', 'Floor-1'),
(2, 'Drill-Z50', '2021-03-22', 'Floor-1'),
(3, 'Conveyor-A1', '2019-11-05', 'Floor-2'),
(4, 'Press-X100', '2022-06-10', 'Floor-2'),
(5, 'Robot-Arm-M3', '2023-01-20', 'Floor-3');

INSERT INTO ManufacturingDB.Bronze.SensorReadings (ReadingID, MachineID, "Timestamp", VibrationLevel, Temperature) VALUES
(1, 1, '2025-01-01 08:00:00', 0.5, 65.0),
(2, 1, '2025-01-01 09:00:00', 0.6, 60.5),
(3, 1, '2025-01-01 10:00:00', 2.8, 85.0), -- Anomaly?
(4, 2, '2025-01-01 08:15:00', 0.2, 55.0),
(5, 3, '2025-01-01 08:30:00', 0.1, 50.0),
(6, 4, '2025-01-01 09:00:00', 0.5, 62.0),
(7, 5, '2025-01-01 09:30:00', 0.3, 58.0),
(8, 1, '2025-01-02 08:00:00', 3.1, 90.0), -- Critical
(9, 2, '2025-01-02 08:00:00', 0.2, 54.0),
(10, 3, '2025-01-02 08:00:00', 0.1, 51.0);

INSERT INTO ManufacturingDB.Bronze.MaintenanceLogs (LogID, MachineID, MaintenanceDate, Type, Cost) VALUES
(1, 1, '2024-12-01', 'Routine', 200.00),
(2, 2, '2024-11-15', 'Routine', 150.00),
(3, 3, '2024-10-30', 'Repair', 500.00),
(4, 1, '2025-01-03', 'Repair', 1200.00), -- Post-anomaly repair
(5, 5, '2024-05-20', 'Routine', 300.00);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Enriched Telemetry
-------------------------------------------------------------------------------

-- 2.1 View: Machine_Health_Status
-- Flags readings that exceed safety thresholds (Vibration > 2.0 or Temp > 80.0)
CREATE OR REPLACE VIEW ManufacturingDB.Silver.Machine_Health_Status AS
SELECT
    m.MachineID,
    m.Model,
    m.Location,
    s."Timestamp",
    s.VibrationLevel,
    s.Temperature,
    CASE 
        WHEN s.VibrationLevel > 2.0 OR s.Temperature > 80.0 THEN 'CRITICAL'
        WHEN s.VibrationLevel > 1.0 OR s.Temperature > 70.0 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS Status
FROM ManufacturingDB.Bronze.Machines m
JOIN ManufacturingDB.Bronze.SensorReadings s ON m.MachineID = s.MachineID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Reliability Metrics
-------------------------------------------------------------------------------

-- 3.1 View: Reliability_Metrics
-- Aggregates critical incidents and maintenance costs.
CREATE OR REPLACE VIEW ManufacturingDB.Gold.Reliability_Metrics AS
SELECT
    m.Model,
    COUNT(DISTINCT hs.MachineID) AS ActiveMachines,
    SUM(CASE WHEN hs.Status = 'CRITICAL' THEN 1 ELSE 0 END) AS CriticalEvents,
    COALESCE(SUM(ml.Cost), 0) AS TotalMaintenanceCost
FROM ManufacturingDB.Bronze.Machines m
JOIN ManufacturingDB.Silver.Machine_Health_Status hs ON m.MachineID = hs.MachineID
LEFT JOIN ManufacturingDB.Bronze.MaintenanceLogs ml ON m.MachineID = ml.MachineID
GROUP BY m.Model;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Real-time Monitoring):
"Show me all machines currently in 'CRITICAL' status from ManufacturingDB.Silver.Machine_Health_Status."

PROMPT 2 (Cost Analysis):
"Using ManufacturingDB.Gold.Reliability_Metrics, which machine model has the highest TotalMaintenanceCost?"
*/
