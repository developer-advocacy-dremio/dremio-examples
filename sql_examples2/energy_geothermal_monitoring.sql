/*
 * Energy: Geothermal Well Integrity Monitoring
 * 
 * Scenario:
 * Monitoring downhole pressure and temperature sensors in deep-earth geothermal injection wells to prevent casing failures.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS GeoEnergyDB;
CREATE FOLDER IF NOT EXISTS GeoEnergyDB.Operations;
CREATE FOLDER IF NOT EXISTS GeoEnergyDB.Operations.Bronze;
CREATE FOLDER IF NOT EXISTS GeoEnergyDB.Operations.Silver;
CREATE FOLDER IF NOT EXISTS GeoEnergyDB.Operations.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Downhole Sensors
-------------------------------------------------------------------------------

-- WellSensors Table
CREATE TABLE IF NOT EXISTS GeoEnergyDB.Operations.Bronze.WellSensors (
    WellID VARCHAR,
    DepthMeters INT, -- 1000m, 2000m, 3000m
    TemperatureCelsius DOUBLE,
    PressurePsi DOUBLE,
    VibrationHz DOUBLE,
    ReadingTimestamp TIMESTAMP
);

INSERT INTO GeoEnergyDB.Operations.Bronze.WellSensors VALUES
('W-001', 1000, 150.0, 2500.0, 2.0, '2025-11-01 08:00:00'),
('W-001', 2000, 220.0, 4500.0, 2.5, '2025-11-01 08:00:00'),
('W-001', 3000, 300.0, 6500.0, 3.0, '2025-11-01 08:00:00'),
('W-002', 1000, 145.0, 2480.0, 1.8, '2025-11-01 08:00:00'),
('W-002', 2000, 215.0, 4450.0, 2.0, '2025-11-01 08:00:00'),
('W-002', 3000, 290.0, 6400.0, 2.2, '2025-11-01 08:00:00'),
('W-003', 1000, 160.0, 2600.0, 5.0, '2025-11-01 08:00:00'), -- High Vib
('W-003', 2000, 230.0, 4800.0, 5.5, '2025-11-01 08:00:00'), -- High Vib
('W-003', 3000, 320.0, 7000.0, 6.0, '2025-11-01 08:00:00'), -- Critical Alert
('W-004', 1000, 150.0, 2500.0, 2.0, '2025-11-01 08:00:00'),
('W-004', 2000, 220.0, 4500.0, 2.2, '2025-11-01 08:00:00'),
('W-004', 3000, 305.0, 6550.0, 2.4, '2025-11-01 08:00:00'),
('W-005', 1000, 148.0, 2490.0, 1.9, '2025-11-01 08:00:00'),
('W-005', 2000, 218.0, 4480.0, 2.1, '2025-11-01 08:00:00'),
('W-005', 3000, 295.0, 6450.0, 2.3, '2025-11-01 08:00:00'),
('W-006', 1000, 152.0, 2510.0, 2.1, '2025-11-01 08:00:00'),
('W-006', 2000, 222.0, 4520.0, 2.3, '2025-11-01 08:00:00'),
('W-006', 3000, 302.0, 6520.0, 2.5, '2025-11-01 08:00:00'),
('W-007', 1000, 155.0, 2550.0, 2.2, '2025-11-01 08:00:00'),
('W-007', 2000, 225.0, 4550.0, 2.4, '2025-11-01 08:00:00'),
('W-007', 3000, 310.0, 6600.0, 2.6, '2025-11-01 08:00:00'),
('W-008', 1000, 153.0, 2530.0, 2.0, '2025-11-01 08:00:00'),
('W-008', 2000, 223.0, 4530.0, 2.2, '2025-11-01 08:00:00'),
('W-008', 3000, 308.0, 6580.0, 2.4, '2025-11-01 08:00:00'),
('W-009', 1000, 151.0, 2505.0, 2.0, '2025-11-01 08:00:00'),
('W-009', 2000, 221.0, 4505.0, 2.2, '2025-11-01 08:00:00'),
('W-009', 3000, 301.0, 6505.0, 2.4, '2025-11-01 08:00:00'),
('W-010', 1000, 149.0, 2495.0, 1.9, '2025-11-01 08:00:00'),
('W-010', 2000, 219.0, 4495.0, 2.1, '2025-11-01 08:00:00'),
('W-010', 3000, 299.0, 6495.0, 2.3, '2025-11-01 08:00:00'),
('W-001', 1000, 152.0, 2510.0, 2.1, '2025-11-01 09:00:00'),
('W-001', 2000, 222.0, 4510.0, 2.6, '2025-11-01 09:00:00'),
('W-001', 3000, 302.0, 6510.0, 3.1, '2025-11-01 09:00:00'),
('W-003', 1000, 165.0, 2700.0, 6.0, '2025-11-01 09:00:00'), -- Escalating
('W-003', 2000, 235.0, 4900.0, 6.5, '2025-11-01 09:00:00'),
('W-003', 3000, 325.0, 7100.0, 7.0, '2025-11-01 09:00:00'),
('W-005', 1000, 148.0, 2490.0, 1.9, '2025-11-01 09:00:00'),
('W-005', 2000, 218.0, 4480.0, 2.1, '2025-11-01 09:00:00'),
('W-005', 3000, 295.0, 6450.0, 2.3, '2025-11-01 09:00:00'),
('W-006', 1000, 153.0, 2520.0, 2.2, '2025-11-01 09:00:00'),
('W-006', 2000, 223.0, 4530.0, 2.4, '2025-11-01 09:00:00'),
('W-006', 3000, 303.0, 6530.0, 2.6, '2025-11-01 09:00:00'),
('W-007', 1000, 156.0, 2560.0, 2.3, '2025-11-01 09:00:00'),
('W-007', 2000, 226.0, 4560.0, 2.5, '2025-11-01 09:00:00'),
('W-007', 3000, 311.0, 6610.0, 2.7, '2025-11-01 09:00:00'),
('W-008', 1000, 154.0, 2540.0, 2.1, '2025-11-01 09:00:00'),
('W-008', 2000, 224.0, 4540.0, 2.3, '2025-11-01 09:00:00'),
('W-008', 3000, 308.0, 6580.0, 2.4, '2025-11-01 09:00:00'),
('W-009', 1000, 151.0, 2505.0, 2.0, '2025-11-01 09:00:00'),
('W-009', 2000, 221.0, 4505.0, 2.2, '2025-11-01 09:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Operation Thresholds
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW GeoEnergyDB.Operations.Silver.WellDiagnostics AS
SELECT 
    WellID,
    DepthMeters,
    TemperatureCelsius,
    PressurePsi,
    VibrationHz,
    ReadingTimestamp,
    -- Threshold Logic
    CASE 
        WHEN TemperatureCelsius > 310 OR PressurePsi > 6800 THEN 'Critical Overpressure'
        WHEN VibrationHz > 4.0 THEN 'Seismic Risk / Drillbit Wear'
        ELSE 'Stable'
    END AS OperationalStatus
FROM GeoEnergyDB.Operations.Bronze.WellSensors;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Integrity Dashboard
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW GeoEnergyDB.Operations.Gold.WellIntegrityMap AS
SELECT 
    WellID,
    SUM(CASE WHEN OperationalStatus != 'Stable' THEN 1 ELSE 0 END) AS AlertCount,
    MAX(TemperatureCelsius) AS PeakTemp,
    MAX(PressurePsi) AS PeakPressure,
    AVG(VibrationHz) AS AvgVibration,
    CASE 
        WHEN MAX(TemperatureCelsius) > 315 THEN 'Shut-In Required'
        WHEN SUM(CASE WHEN OperationalStatus != 'Stable' THEN 1 ELSE 0 END) > 0 THEN 'Maintenance Due'
        ELSE 'Operational'
    END AS ActionRequired
FROM GeoEnergyDB.Operations.Silver.WellDiagnostics
GROUP BY WellID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify all wells marked as 'Shut-In Required' in the Gold integrity dashboard."

PROMPT 2:
"List all sensor readings from the Silver layer where the status is 'Seismic Risk / Drillbit Wear'."

PROMPT 3:
"Calculate the average pressure for WellID 'W-001' at 3000m depth."
*/
