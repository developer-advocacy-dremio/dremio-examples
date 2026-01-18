/*
    Dremio High-Volume SQL Pattern: Healthcare ED Diversion Analysis
    
    Business Scenario:
    When an Emergency Department (ED) goes on "Diversion" (turning away ambulances due to capacity),
    it loses revenue and impacts community safety. Understanding root causes (e.g., CT Down, No Beds) is vital.
    
    Data Story:
    We track Diversion Logs and Ambulance Arrival Logs.
    
    Medallion Architecture:
    - Bronze: DiversionLogs, ArrivalLogs.
      *Volume*: 50+ records.
    - Silver: LostRevenueEst (Estimating admitted patients lost).
    - Gold: RootCauseAnalysis (Hours on diversion by Reason).
    
    Key Dremio Features:
    - TIMESTAMPDIFF
    - Aggregation by Reason Code
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareDiversionDB;
CREATE FOLDER IF NOT EXISTS HealthcareDiversionDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareDiversionDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareDiversionDB.Gold;
USE HealthcareDiversionDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareDiversionDB.Bronze.DiversionLogs (
    LogID STRING,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    ReasonCode STRING -- NO_BEDS, CT_DOWN, PSYCH_HOLD
);

INSERT INTO HealthcareDiversionDB.Bronze.DiversionLogs VALUES
('L001', TIMESTAMP '2025-01-01 10:00:00', TIMESTAMP '2025-01-01 14:00:00', 'NO_BEDS'), -- 4h
('L002', TIMESTAMP '2025-01-02 08:00:00', TIMESTAMP '2025-01-02 10:00:00', 'CT_DOWN'), -- 2h
('L003', TIMESTAMP '2025-01-03 18:00:00', TIMESTAMP '2025-01-03 22:00:00', 'NO_BEDS'), -- 4h
('L004', TIMESTAMP '2025-01-05 12:00:00', TIMESTAMP '2025-01-05 16:00:00', 'PSYCH_HOLD'); -- 4h

-- Bulk Diversions (Simulating a bad month)
INSERT INTO HealthcareDiversionDB.Bronze.DiversionLogs
SELECT 
  'L' || CAST(rn + 10 AS STRING),
  DATE_ADD(TIMESTAMP '2025-01-05 12:00:00', CAST(rn AS INT) * 1000 * 60 * 60 * 24), -- Daily incidents
  DATE_ADD(TIMESTAMP '2025-01-05 16:00:00', CAST(rn AS INT) * 1000 * 60 * 60 * 24), -- 4h duration
  CASE WHEN rn % 3 = 0 THEN 'NO_BEDS' WHEN rn % 3 = 1 THEN 'CT_DOWN' ELSE 'STAFFING' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Duration Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareDiversionDB.Silver.DiversionHours AS
SELECT
    LogID,
    ReasonCode,
    StartTime,
    EndTime,
    TIMESTAMPDIFF(MINUTE, StartTime, EndTime) / 60.0 AS DurationHours,
    -- Est 2 ambulances lost per hour
    (TIMESTAMPDIFF(MINUTE, StartTime, EndTime) / 60.0) * 2 AS EstAmbulancesLost
FROM HealthcareDiversionDB.Bronze.DiversionLogs;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Root Cause & Impact
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareDiversionDB.Gold.DiversionImpactAnalysis AS
SELECT
    ReasonCode,
    COUNT(LogID) AS IncidentCount,
    SUM(DurationHours) AS TotalHours,
    SUM(EstAmbulancesLost) AS TotalAmbulancesLost,
    SUM(EstAmbulancesLost) * 5000 AS EstRevenueImpactUSD -- Est $5k per admit
FROM HealthcareDiversionDB.Silver.DiversionHours
GROUP BY ReasonCode;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "What is the primary reason for ED Diversion?"
    2. "Calculate the total estimated revenue lost due to 'NO_BEDS'."
    3. "Show the trend of diversion hours by date."
*/
