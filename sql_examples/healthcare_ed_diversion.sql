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
('L004', TIMESTAMP '2025-01-05 12:00:00', TIMESTAMP '2025-01-05 16:00:00', 'PSYCH_HOLD'), -- 4h
('L005', TIMESTAMP '2025-01-06 09:00:00', TIMESTAMP '2025-01-06 13:00:00', 'NO_BEDS'),
('L006', TIMESTAMP '2025-01-07 14:00:00', TIMESTAMP '2025-01-07 16:00:00', 'STAFFING'),
('L007', TIMESTAMP '2025-01-08 20:00:00', TIMESTAMP '2025-01-08 23:00:00', 'NO_BEDS'),
('L008', TIMESTAMP '2025-01-09 10:00:00', TIMESTAMP '2025-01-09 12:00:00', 'CT_DOWN'),
('L009', TIMESTAMP '2025-01-10 11:00:00', TIMESTAMP '2025-01-10 15:00:00', 'NO_BEDS'),
('L010', TIMESTAMP '2025-01-11 16:00:00', TIMESTAMP '2025-01-11 18:00:00', 'PSYCH_HOLD'),
('L011', TIMESTAMP '2025-01-12 08:00:00', TIMESTAMP '2025-01-12 12:00:00', 'NO_BEDS'),
('L012', TIMESTAMP '2025-01-13 13:00:00', TIMESTAMP '2025-01-13 14:00:00', 'STAFFING'),
('L013', TIMESTAMP '2025-01-14 18:00:00', TIMESTAMP '2025-01-14 22:00:00', 'NO_BEDS'),
('L014', TIMESTAMP '2025-01-15 09:00:00', TIMESTAMP '2025-01-15 11:00:00', 'CT_DOWN'),
('L015', TIMESTAMP '2025-01-16 10:00:00', TIMESTAMP '2025-01-16 16:00:00', 'NO_BEDS'),
('L016', TIMESTAMP '2025-01-17 14:00:00', TIMESTAMP '2025-01-17 18:00:00', 'NO_BEDS'),
('L017', TIMESTAMP '2025-01-18 20:00:00', TIMESTAMP '2025-01-18 22:00:00', 'PSYCH_HOLD'),
('L018', TIMESTAMP '2025-01-19 12:00:00', TIMESTAMP '2025-01-19 14:00:00', 'STAFFING'),
('L019', TIMESTAMP '2025-01-20 08:00:00', TIMESTAMP '2025-01-20 12:00:00', 'NO_BEDS'),
('L020', TIMESTAMP '2025-01-21 16:00:00', TIMESTAMP '2025-01-21 20:00:00', 'NO_BEDS'),
('L021', TIMESTAMP '2025-01-22 10:00:00', TIMESTAMP '2025-01-22 13:00:00', 'CT_DOWN'),
('L022', TIMESTAMP '2025-01-23 09:00:00', TIMESTAMP '2025-01-23 15:00:00', 'NO_BEDS'),
('L023', TIMESTAMP '2025-01-24 18:00:00', TIMESTAMP '2025-01-24 21:00:00', 'PSYCH_HOLD'),
('L024', TIMESTAMP '2025-01-25 12:00:00', TIMESTAMP '2025-01-25 16:00:00', 'NO_BEDS'),
('L025', TIMESTAMP '2025-01-26 08:00:00', TIMESTAMP '2025-01-26 11:00:00', 'STAFFING'),
('L026', TIMESTAMP '2025-01-27 14:00:00', TIMESTAMP '2025-01-27 18:00:00', 'NO_BEDS'),
('L027', TIMESTAMP '2025-01-28 20:00:00', TIMESTAMP '2025-01-28 22:00:00', 'CT_DOWN'),
('L028', TIMESTAMP '2025-01-29 10:00:00', TIMESTAMP '2025-01-29 14:00:00', 'NO_BEDS'),
('L029', TIMESTAMP '2025-01-30 15:00:00', TIMESTAMP '2025-01-30 18:00:00', 'PSYCH_HOLD'),
('L030', TIMESTAMP '2025-01-31 09:00:00', TIMESTAMP '2025-01-31 16:00:00', 'NO_BEDS'),
('L031', TIMESTAMP '2025-02-01 10:00:00', TIMESTAMP '2025-02-01 12:00:00', 'STAFFING'),
('L032', TIMESTAMP '2025-02-02 18:00:00', TIMESTAMP '2025-02-02 23:00:00', 'NO_BEDS'),
('L033', TIMESTAMP '2025-02-03 12:00:00', TIMESTAMP '2025-02-03 14:00:00', 'CT_DOWN'),
('L034', TIMESTAMP '2025-02-04 08:00:00', TIMESTAMP '2025-02-04 12:00:00', 'NO_BEDS'),
('L035', TIMESTAMP '2025-02-05 16:00:00', TIMESTAMP '2025-02-05 19:00:00', 'PSYCH_HOLD'),
('L036', TIMESTAMP '2025-02-06 14:00:00', TIMESTAMP '2025-02-06 16:00:00', 'STAFFING'),
('L037', TIMESTAMP '2025-02-07 20:00:00', TIMESTAMP '2025-02-07 23:59:00', 'NO_BEDS'),
('L038', TIMESTAMP '2025-02-08 10:00:00', TIMESTAMP '2025-02-08 15:00:00', 'NO_BEDS'),
('L039', TIMESTAMP '2025-02-09 11:00:00', TIMESTAMP '2025-02-09 13:00:00', 'CT_DOWN'),
('L040', TIMESTAMP '2025-02-10 17:00:00', TIMESTAMP '2025-02-10 20:00:00', 'NO_BEDS'),
('L041', TIMESTAMP '2025-02-11 09:00:00', TIMESTAMP '2025-02-11 11:00:00', 'STAFFING'),
('L042', TIMESTAMP '2025-02-12 13:00:00', TIMESTAMP '2025-02-12 18:00:00', 'NO_BEDS'),
('L043', TIMESTAMP '2025-02-13 19:00:00', TIMESTAMP '2025-02-13 22:00:00', 'PSYCH_HOLD'),
('L044', TIMESTAMP '2025-02-14 10:00:00', TIMESTAMP '2025-02-14 12:00:00', 'CT_DOWN'),
('L045', TIMESTAMP '2025-02-15 15:00:00', TIMESTAMP '2025-02-15 19:00:00', 'NO_BEDS'),
('L046', TIMESTAMP '2025-02-16 08:00:00', TIMESTAMP '2025-02-16 12:00:00', 'NO_BEDS'),
('L047', TIMESTAMP '2025-02-17 14:00:00', TIMESTAMP '2025-02-17 16:00:00', 'STAFFING'),
('L048', TIMESTAMP '2025-02-18 20:00:00', TIMESTAMP '2025-02-18 23:00:00', 'PSYCH_HOLD'),
('L049', TIMESTAMP '2025-02-19 11:00:00', TIMESTAMP '2025-02-19 15:00:00', 'NO_BEDS'),
('L050', TIMESTAMP '2025-02-20 18:00:00', TIMESTAMP '2025-02-20 20:00:00', 'CT_DOWN');

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
