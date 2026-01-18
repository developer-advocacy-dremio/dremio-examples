/*
    Dremio High-Volume SQL Pattern: Government Water Quality
    
    Business Scenario:
    Utilities monitor "Contaminants" (Lead, Copper, PFAS) to prevent public health crises.
    Tracking "Safety Breaches" in real-time allows for immediate advisories.
    
    Data Story:
    We track Sampling Stations and Test Results.
    
    Medallion Architecture:
    - Bronze: Stations, LabResults.
      *Volume*: 50+ records.
    - Silver: ViolationEvents (Result > Limit).
    - Gold: MonthlySafetyReport (Compliance rate).
    
    Key Dremio Features:
    - Filtered Aggregation
    - Case Logic
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentWaterDB;
CREATE FOLDER IF NOT EXISTS GovernmentWaterDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentWaterDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentWaterDB.Gold;
USE GovernmentWaterDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentWaterDB.Bronze.Contaminants (
    ContaminantCode STRING,
    Name STRING,
    SafetyLimitPPB DOUBLE -- Parts Per Billion
);

INSERT INTO GovernmentWaterDB.Bronze.Contaminants VALUES
('PB', 'Lead', 15.0),
('CU', 'Copper', 1300.0),
('PFAS', 'Perfluoroalkyl', 70.0);

CREATE OR REPLACE TABLE GovernmentWaterDB.Bronze.LabResults (
    SampleID STRING,
    StationID STRING,
    ContaminantCode STRING,
    SampleDate DATE,
    ResultValuePPB DOUBLE
);

-- Bulk Samples (50 Records)
INSERT INTO GovernmentWaterDB.Bronze.LabResults VALUES
('S5001', 'STATION_1', 'PB', DATE '2025-01-01', 5.0),
('S5002', 'STATION_1', 'CU', DATE '2025-01-01', 100.0),
('S5003', 'STATION_1', 'PB', DATE '2025-01-08', 20.0), -- Lead Fail
('S5004', 'STATION_2', 'PFAS', DATE '2025-01-02', 10.0),
('S5005', 'STATION_2', 'PB', DATE '2025-01-05', 12.0),
('S5006', 'STATION_3', 'CU', DATE '2025-01-03', 1400.0), -- Copper Fail
('S5007', 'STATION_3', 'PFAS', DATE '2025-01-10', 80.0), -- PFAS Fail
('S5008', 'STATION_1', 'PB', DATE '2025-01-15', 8.0),
('S5009', 'STATION_1', 'CU', DATE '2025-01-15', 500.0),
('S5010', 'STATION_2', 'PB', DATE '2025-01-12', 4.0),
('S5011', 'STATION_2', 'PFAS', DATE '2025-01-18', 50.0),
('S5012', 'STATION_3', 'PB', DATE '2025-01-18', 9.0),
('S5013', 'STATION_4', 'PB', DATE '2025-01-01', 2.0),
('S5014', 'STATION_4', 'CU', DATE '2025-01-02', 60.0),
('S5015', 'STATION_5', 'PB', DATE '2025-01-03', 25.0), -- Lead Fail
('S5016', 'STATION_5', 'PFAS', DATE '2025-01-04', 100.0), -- PFAS Fail
('S5017', 'STATION_4', 'PB', DATE '2025-01-10', 3.0),
('S5018', 'STATION_5', 'PB', DATE '2025-01-15', 18.0), -- Lead Fail
('S5019', 'STATION_1', 'PB', DATE '2025-01-20', 6.0),
('S5020', 'STATION_2', 'CU', DATE '2025-01-20', 200.0),
('S5021', 'STATION_3', 'PB', DATE '2025-01-05', 7.0),
('S5022', 'STATION_3', 'CU', DATE '2025-01-12', 1200.0),
('S5023', 'STATION_6', 'PB', DATE '2025-01-01', 1.0),
('S5024', 'STATION_6', 'PFAS', DATE '2025-01-08', 20.0),
('S5025', 'STATION_6', 'CU', DATE '2025-01-15', 800.0),
('S5026', 'STATION_1', 'PFAS', DATE '2025-01-05', 60.0),
('S5027', 'STATION_2', 'PB', DATE '2025-01-08', 3.0),
('S5028', 'STATION_4', 'PFAS', DATE '2025-01-12', 40.0),
('S5029', 'STATION_5', 'CU', DATE '2025-01-02', 2000.0), -- Big Copper Fail
('S5030', 'STATION_6', 'PB', DATE '2025-01-18', 10.0),
('S5031', 'STATION_1', 'CU', DATE '2025-01-10', 900.0),
('S5032', 'STATION_2', 'PB', DATE '2025-01-15', 5.0),
('S5033', 'STATION_3', 'PB', DATE '2025-01-01', 11.0),
('S5034', 'STATION_4', 'CU', DATE '2025-01-18', 700.0),
('S5035', 'STATION_5', 'PB', DATE '2025-01-20', 14.0),
('S5036', 'STATION_6', 'PFAS', DATE '2025-01-20', 30.0),
('S5037', 'STATION_1', 'PB', DATE '2025-01-02', 4.0),
('S5038', 'STATION_2', 'CU', DATE '2025-01-04', 300.0),
('S5039', 'STATION_3', 'PFAS', DATE '2025-01-15', 85.0), -- PFAS Fail
('S5040', 'STATION_4', 'PB', DATE '2025-01-14', 6.0),
('S5041', 'STATION_5', 'CU', DATE '2025-01-08', 1500.0), -- Copper Fail
('S5042', 'STATION_6', 'PB', DATE '2025-01-10', 3.0),
('S5043', 'STATION_1', 'PB', DATE '2025-01-12', 5.0),
('S5044', 'STATION_2', 'PFAS', DATE '2025-01-12', 45.0),
('S5045', 'STATION_3', 'CU', DATE '2025-01-08', 1100.0),
('S5046', 'STATION_4', 'PB', DATE '2025-01-16', 7.0),
('S5047', 'STATION_5', 'PFAS', DATE '2025-01-18', 95.0), -- PFAS Fail
('S5048', 'STATION_6', 'CU', DATE '2025-01-05', 500.0),
('S5049', 'STATION_1', 'PB', DATE '2025-01-18', 4.0),
('S5050', 'STATION_2', 'CU', DATE '2025-01-18', 250.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Breach Detection
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentWaterDB.Silver.SafetyViolations AS
SELECT
    r.SampleID,
    r.StationID,
    r.SampleDate,
    c.Name,
    r.ResultValuePPB,
    c.SafetyLimitPPB,
    (r.ResultValuePPB - c.SafetyLimitPPB) AS ExcessAmount
FROM GovernmentWaterDB.Bronze.LabResults r
JOIN GovernmentWaterDB.Bronze.Contaminants c ON r.ContaminantCode = c.ContaminantCode
WHERE r.ResultValuePPB > c.SafetyLimitPPB;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Compliance Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentWaterDB.Gold.WaterReport AS
SELECT
    StationID,
    COUNT(*) AS TotalSamples,
    SUM(CASE WHEN ResultValuePPB > SafetyLimitPPB THEN 1 ELSE 0 END) AS FailedSamples,
    MAX(ResultValuePPB) AS MaxReadingDetected,
    -- Join required for limit, simplified here assumption
    (CAST(SUM(CASE WHEN ResultValuePPB > SafetyLimitPPB THEN 0 ELSE 1 END) AS DOUBLE) / COUNT(*)) * 100 AS CompliancePct
    -- Note: In real scenarios, join back to get Limit for context
FROM GovernmentWaterDB.Bronze.LabResults r -- Re-joining Bronze for denominator
JOIN GovernmentWaterDB.Bronze.Contaminants c ON r.ContaminantCode = c.ContaminantCode
GROUP BY StationID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all stations with Failed Samples."
    2. "What is the max Lead reading detected?"
    3. "Show compliance percentage by Station."
*/
