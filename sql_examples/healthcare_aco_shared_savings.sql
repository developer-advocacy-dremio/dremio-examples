/*
    Dremio High-Volume SQL Pattern: Healthcare ACO Shared Savings
    
    Business Scenario:
    Accountable Care Organizations (ACOs) are rewarded for keeping costs below a Benchmark
    while maintaining quality. Tracking PMPY (Per Member Per Year) spend is critical.
    
    Data Story:
    We track Claims Costs and the Member Roster.
    
    Medallion Architecture:
    - Bronze: ClaimsCost, MemberRoster.
      *Volume*: 50+ records.
    - Silver: PMPY_Calc (Spend per member).
    - Gold: SavingsProjection (Actual vs Benchmark).
    
    Key Dremio Features:
    - Aggregation
    - Financial Formatting
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareACODB;
CREATE FOLDER IF NOT EXISTS HealthcareACODB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareACODB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareACODB.Gold;
USE HealthcareACODB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareACODB.Bronze.MemberRoster (
    MemberID STRING,
    AttributionYear INT,
    RiskScore DOUBLE -- 1.0 is avg
);

INSERT INTO HealthcareACODB.Bronze.MemberRoster VALUES
('M001', 2024, 1.2),
('M002', 2024, 0.8),
('M003', 2024, 2.5),
('M004', 2024, 1.0),
('M005', 2024, 1.5),
('M006', 2024, 0.9), -- Low Risk
('M007', 2024, 3.0), -- High Risk
('M008', 2024, 1.1),
('M009', 2024, 1.3),
('M010', 2024, 0.7),
('M011', 2024, 1.4),
('M012', 2024, 0.6),
('M013', 2024, 2.1),
('M014', 2024, 1.2),
('M015', 2024, 1.6),
('M016', 2024, 0.8),
('M017', 2024, 2.8),
('M018', 2024, 1.0),
('M019', 2024, 1.3),
('M020', 2024, 0.5),
('M021', 2024, 1.2),
('M022', 2024, 0.9),
('M023', 2024, 2.3),
('M024', 2024, 1.1),
('M025', 2024, 1.4),
('M026', 2024, 0.8),
('M027', 2024, 2.9),
('M028', 2024, 1.0),
('M029', 2024, 1.5),
('M030', 2024, 0.6),
('M031', 2024, 1.3),
('M032', 2024, 0.7),
('M033', 2024, 2.2),
('M034', 2024, 1.2),
('M035', 2024, 1.7),
('M036', 2024, 0.8),
('M037', 2024, 2.6),
('M038', 2024, 0.9),
('M039', 2024, 1.4),
('M040', 2024, 0.5),
('M041', 2024, 1.1),
('M042', 2024, 0.8),
('M043', 2024, 2.4),
('M044', 2024, 1.3),
('M045', 2024, 1.5),
('M046', 2024, 0.7),
('M047', 2024, 2.7),
('M048', 2024, 1.0),
('M049', 2024, 1.2),
('M050', 2024, 0.6);

CREATE OR REPLACE TABLE HealthcareACODB.Bronze.ClaimsCost (
    ClaimID STRING,
    MemberID STRING,
    TotalCost DOUBLE,
    Category STRING -- Inpatient, Outpatient, Rx
);

INSERT INTO HealthcareACODB.Bronze.ClaimsCost VALUES
('C1001', 'M001', 5000.00, 'Inpatient'),
('C1002', 'M002', 200.00, 'Rx'),
('C1003', 'M003', 25000.00, 'Inpatient'), -- High Cost
('C1004', 'M001', 150.00, 'Rx'),
('C1005', 'M004', 1200.00, 'Outpatient'),
('C1006', 'M005', 3000.00, 'Inpatient'),
('C1007', 'M006', 100.00, 'Rx'),
('C1008', 'M007', 45000.00, 'Inpatient'), -- High Cost
('C1009', 'M008', 500.00, 'Outpatient'),
('C1010', 'M009', 2500.00, 'Inpatient'),
('C1011', 'M010', 50.00, 'Rx'),
('C1012', 'M011', 1800.00, 'Outpatient'),
('C1013', 'M012', 300.00, 'Rx'),
('C1014', 'M013', 12000.00, 'Inpatient'),
('C1015', 'M014', 600.00, 'Outpatient'),
('C1016', 'M015', 4000.00, 'Inpatient'),
('C1017', 'M016', 150.00, 'Rx'),
('C1018', 'M017', 32000.00, 'Inpatient'),
('C1019', 'M018', 700.00, 'Outpatient'),
('C1020', 'M019', 2800.00, 'Inpatient'),
('C1021', 'M020', 80.00, 'Rx'),
('C1022', 'M021', 1500.00, 'Outpatient'),
('C1023', 'M022', 250.00, 'Rx'),
('C1024', 'M023', 18000.00, 'Inpatient'),
('C1025', 'M024', 900.00, 'Outpatient'),
('C1026', 'M025', 3500.00, 'Inpatient'),
('C1027', 'M026', 120.00, 'Rx'),
('C1028', 'M027', 41000.00, 'Inpatient'),
('C1029', 'M028', 800.00, 'Outpatient'),
('C1030', 'M029', 2900.00, 'Inpatient'),
('C1031', 'M030', 90.00, 'Rx'),
('C1032', 'M031', 2000.00, 'Outpatient'),
('C1033', 'M032', 300.00, 'Rx'),
('C1034', 'M033', 15000.00, 'Inpatient'),
('C1035', 'M034', 1100.00, 'Outpatient'),
('C1036', 'M035', 4200.00, 'Inpatient'),
('C1037', 'M036', 140.00, 'Rx'),
('C1038', 'M037', 38000.00, 'Inpatient'),
('C1039', 'M038', 650.00, 'Outpatient'),
('C1040', 'M039', 2700.00, 'Inpatient'),
('C1041', 'M040', 70.00, 'Rx'),
('C1042', 'M041', 1300.00, 'Outpatient'),
('C1043', 'M042', 200.00, 'Rx'),
('C1044', 'M043', 19000.00, 'Inpatient'),
('C1045', 'M044', 1000.00, 'Outpatient'),
('C1046', 'M045', 3800.00, 'Inpatient'),
('C1047', 'M046', 110.00, 'Rx'),
('C1048', 'M047', 35000.00, 'Inpatient'),
('C1049', 'M048', 750.00, 'Outpatient'),
('C1050', 'M049', 2600.00, 'Inpatient');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: PMPY Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareACODB.Silver.MemberSpend AS
SELECT
    m.MemberID,
    m.RiskScore,
    COALESCE(SUM(c.TotalCost), 0) AS TotalSpend,
    -- Benchmark: $10,000 * RiskScore
    (10000.0 * m.RiskScore) AS BenchmarkSpend
FROM HealthcareACODB.Bronze.MemberRoster m
LEFT JOIN HealthcareACODB.Bronze.ClaimsCost c ON m.MemberID = c.MemberID
GROUP BY m.MemberID, m.RiskScore;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Savings Projection
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareACODB.Gold.ACOPerformance AS
SELECT
    COUNT(MemberID) AS TotalMembers,
    SUM(TotalSpend) AS ActualTotalSpend,
    SUM(BenchmarkSpend) AS BenchmarkTotalSpend,
    (SUM(BenchmarkSpend) - SUM(TotalSpend)) AS SharedSavingsPool,
    CASE WHEN SUM(TotalSpend) < SUM(BenchmarkSpend) THEN 'Profitable' ELSE 'Loss' END AS Outcome
FROM HealthcareACODB.Silver.MemberSpend;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "What is the projected Shared Savings Pool?"
    2. "List members with spend exceeding their benchmark."
    3. "Compare Actual vs Benchmark spend by Risk Score range."
*/
