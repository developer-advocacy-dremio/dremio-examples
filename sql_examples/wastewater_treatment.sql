/*
 * Wastewater Treatment Demo
 * 
 * Scenario:
 * Monitoring effluent quality, chemical dosage, and flow rates.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Ensure environmental compliance and optimize chemical costs.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS WastewaterDB;
CREATE FOLDER IF NOT EXISTS WastewaterDB.Bronze;
CREATE FOLDER IF NOT EXISTS WastewaterDB.Silver;
CREATE FOLDER IF NOT EXISTS WastewaterDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS WastewaterDB.Bronze.Sensors (
    SensorID VARCHAR,
    Location VARCHAR, -- 'Influent', 'Aeration', 'Effluent'
    "Timestamp" TIMESTAMP,
    Metric VARCHAR, -- 'pH', 'BOD', 'TSS'
    Value DOUBLE
);

CREATE TABLE IF NOT EXISTS WastewaterDB.Bronze.ChemicalDosing (
    BatchID INT,
    ChemicalType VARCHAR,
    AmountGallons DOUBLE,
    "Timestamp" TIMESTAMP
);

INSERT INTO WastewaterDB.Bronze.Sensors VALUES
('S1', 'Effluent', '2025-06-01 10:00:00', 'pH', 7.2), -- Normal
('S1', 'Effluent', '2025-06-01 11:00:00', 'pH', 6.5), -- Warning
('S2', 'Influent', '2025-06-01 10:00:00', 'BOD', 300.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW WastewaterDB.Silver.HourlyQuality AS
SELECT 
    Location,
    Metric,
    DATE_TRUNC('HOUR', "Timestamp") AS HourBlock,
    AVG(Value) AS AvgValue,
    MAX(Value) AS MaxValue,
    MIN(Value) AS MinValue
FROM WastewaterDB.Bronze.Sensors
GROUP BY Location, Metric, DATE_TRUNC('HOUR', "Timestamp");

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW WastewaterDB.Gold.ComplianceViolations AS
SELECT 
    HourBlock,
    Metric,
    AvgValue,
    'Violation' AS Status
FROM WastewaterDB.Silver.HourlyQuality
WHERE Location = 'Effluent' 
  AND (Metric = 'pH' AND (AvgValue < 6.0 OR AvgValue > 9.0));

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Check for any pH violations in WastewaterDB.Gold.ComplianceViolations."
*/
