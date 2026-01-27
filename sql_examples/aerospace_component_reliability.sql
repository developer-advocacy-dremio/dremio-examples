/*
 * Aerospace Component Reliability Demo
 * 
 * Scenario:
 * Tracking part Mean-Time-Between-Failures (MTBF) and flight hours.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ManufacturingDB;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Aerospace;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Aerospace.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Aerospace.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Aerospace.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ManufacturingDB.Aerospace.Bronze.Parts (
    PartSerial VARCHAR,
    PartType VARCHAR, -- Engine, LandingGear, Avionics
    InstallDate DATE,
    RemoveDate DATE, -- Null if still installed
    FlightHours INT,
    FailureReason VARCHAR
);

INSERT INTO ManufacturingDB.Aerospace.Bronze.Parts VALUES
('SN001', 'Engine', '2023-01-01', '2024-01-01', 2000, 'Scheduled Maintenance'),
('SN002', 'Engine', '2023-06-01', NULL, 1500, NULL),
('SN003', 'LandingGear', '2023-01-01', '2024-06-01', 3000, 'Wear Limit'),
('SN004', 'Avionics', '2024-01-01', '2024-03-01', 300, 'Component Error'), -- Early failure
('SN005', 'Engine', '2024-02-01', NULL, 800, NULL),
('SN006', 'LandingGear', '2024-01-01', NULL, 1200, NULL),
('SN007', 'Avionics', '2023-01-01', NULL, 4000, NULL),
('SN008', 'Engine', '2022-01-01', '2023-01-01', 2200, 'Scheduled Maintenance'),
('SN009', 'LandingGear', '2022-06-01', '2023-12-01', 2800, 'Hydraulic Leak'),
('SN010', 'Avionics', '2024-05-01', NULL, 500, NULL);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ManufacturingDB.Aerospace.Silver.ComponentHistory AS
SELECT 
    PartType,
    PartSerial,
    FlightHours,
    CASE 
        WHEN FailureReason IS NOT NULL AND FailureReason != 'Scheduled Maintenance' THEN 'Unscheduled Removal'
        ELSE 'Scheduled/Active' 
    END AS RemovalType
FROM ManufacturingDB.Aerospace.Bronze.Parts;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ManufacturingDB.Aerospace.Gold.ReliabilityStats AS
SELECT 
    PartType,
    SUM(FlightHours) AS TotalFlightHours,
    SUM(CASE WHEN RemovalType = 'Unscheduled Removal' THEN 1 ELSE 0 END) AS FailureCount,
    -- MTBF (Mean Time Between Failures)
    SUM(FlightHours) / NULLIF(SUM(CASE WHEN RemovalType = 'Unscheduled Removal' THEN 1 ELSE 0 END), 0) AS CalculatedMTBF
FROM ManufacturingDB.Aerospace.Silver.ComponentHistory
GROUP BY PartType;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Calculate the CalculatedMTBF for 'Avionics' parts in ManufacturingDB.Aerospace.Gold.ReliabilityStats."
*/
