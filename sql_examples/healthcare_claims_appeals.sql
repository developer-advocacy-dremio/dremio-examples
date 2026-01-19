/*
    Dremio High-Volume SQL Pattern: Healthcare Claims Denial Appeals
    
    Business Scenario:
    Hospitals lose millions in denied claims. Tracking the "Overturn Rate" (Success of appeals)
    helps identify which denials are worth fighting vs writing off.
    
    Data Story:
    We track Initial Denials and Appeal Submissions.
    
    Medallion Architecture:
    - Bronze: Denials, Appeals.
      *Volume*: 50+ records.
    - Silver: AppealOutcome (Matched denials to appeal results).
    - Gold: StrategyReport (Success rate by Payer and Reason).
    
    Key Dremio Features:
    - Aggregation
    - Success Rate Calculation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB;
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareAppealsDB.Gold;
USE HealthcareAppealsDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareAppealsDB.Bronze.Denials (
    ClaimID STRING,
    Payer STRING,
    DenialReason STRING, -- Medical Necessity, Authorization, Coding
    DenialAmount DOUBLE,
    DenialDate DATE
);

INSERT INTO HealthcareAppealsDB.Bronze.Denials VALUES
('C1001', 'UHC', 'MedNecessity', 1500.00, DATE '2025-01-01'),
('C1002', 'Aetna', 'NoAuth', 2500.00, DATE '2025-01-02'),
('C1003', 'BCBS', 'CodingError', 800.00, DATE '2025-01-03'),
('C1004', 'UHC', 'MedNecessity', 3000.00, DATE '2025-01-04'),
('C1005', 'Aetna', 'NoAuth', 1200.00, DATE '2025-01-05'),
('C1006', 'BCBS', 'CodingError', 600.00, DATE '2025-01-06'),
('C1007', 'UHC', 'NoAuth', 2200.00, DATE '2025-01-07'),
('C1008', 'Aetna', 'MedNecessity', 4000.00, DATE '2025-01-08'),
('C1009', 'BCBS', 'NoAuth', 1500.00, DATE '2025-01-09'),
('C1010', 'UHC', 'CodingError', 900.00, DATE '2025-01-10'),
('C1011', 'Aetna', 'MedNecessity', 2800.00, DATE '2025-01-11'),
('C1012', 'BCBS', 'CodingError', 700.00, DATE '2025-01-12'),
('C1013', 'UHC', 'NoAuth', 1800.00, DATE '2025-01-13'),
('C1014', 'Aetna', 'MedNecessity', 3500.00, DATE '2025-01-14'),
('C1015', 'BCBS', 'NoAuth', 2000.00, DATE '2025-01-15'),
('C1016', 'UHC', 'CodingError', 500.00, DATE '2025-01-16'),
('C1017', 'Aetna', 'MedNecessity', 4200.00, DATE '2025-01-17'),
('C1018', 'BCBS', 'CodingError', 850.00, DATE '2025-01-18'),
('C1019', 'UHC', 'NoAuth', 1600.00, DATE '2025-01-19'),
('C1020', 'Aetna', 'MedNecessity', 3100.00, DATE '2025-01-20'),
('C1021', 'BCBS', 'NoAuth', 1900.00, DATE '2025-01-21'),
('C1022', 'UHC', 'CodingError', 750.00, DATE '2025-01-22'),
('C1023', 'Aetna', 'MedNecessity', 3800.00, DATE '2025-01-23'),
('C1024', 'BCBS', 'CodingError', 650.00, DATE '2025-01-24'),
('C1025', 'UHC', 'NoAuth', 2400.00, DATE '2025-01-25'),
('C1026', 'Aetna', 'MedNecessity', 4500.00, DATE '2025-01-26'),
('C1027', 'BCBS', 'NoAuth', 1700.00, DATE '2025-01-27'),
('C1028', 'UHC', 'CodingError', 950.00, DATE '2025-01-28'),
('C1029', 'Aetna', 'MedNecessity', 2900.00, DATE '2025-01-29'),
('C1030', 'BCBS', 'CodingError', 800.00, DATE '2025-01-30'),
('C1031', 'UHC', 'NoAuth', 2100.00, DATE '2025-01-31'),
('C1032', 'Aetna', 'MedNecessity', 3600.00, DATE '2025-02-01'),
('C1033', 'BCBS', 'NoAuth', 1400.00, DATE '2025-02-02'),
('C1034', 'UHC', 'CodingError', 600.00, DATE '2025-02-03'),
('C1035', 'Aetna', 'MedNecessity', 4100.00, DATE '2025-02-04'),
('C1036', 'BCBS', 'CodingError', 700.00, DATE '2025-02-05'),
('C1037', 'UHC', 'NoAuth', 2300.00, DATE '2025-02-06'),
('C1038', 'Aetna', 'MedNecessity', 3300.00, DATE '2025-02-07'),
('C1039', 'BCBS', 'NoAuth', 1800.00, DATE '2025-02-08'),
('C1040', 'UHC', 'CodingError', 850.00, DATE '2025-02-09'),
('C1041', 'Aetna', 'MedNecessity', 3900.00, DATE '2025-02-10'),
('C1042', 'BCBS', 'CodingError', 750.00, DATE '2025-02-11'),
('C1043', 'UHC', 'NoAuth', 2000.00, DATE '2025-02-12'),
('C1044', 'Aetna', 'MedNecessity', 4300.00, DATE '2025-02-13'),
('C1045', 'BCBS', 'NoAuth', 1600.00, DATE '2025-02-14'),
('C1046', 'UHC', 'CodingError', 900.00, DATE '2025-02-15'),
('C1047', 'Aetna', 'MedNecessity', 3400.00, DATE '2025-02-16'),
('C1048', 'BCBS', 'CodingError', 650.00, DATE '2025-02-17'),
('C1049', 'UHC', 'NoAuth', 2500.00, DATE '2025-02-18'),
('C1050', 'Aetna', 'MedNecessity', 4600.00, DATE '2025-02-19');

CREATE OR REPLACE TABLE HealthcareAppealsDB.Bronze.Appeals (
    AppealID STRING,
    ClaimID STRING,
    AppealDate DATE,
    Outcome STRING -- Overturned (Win), Upheld (Loss), Pending
);

INSERT INTO HealthcareAppealsDB.Bronze.Appeals VALUES
('A5001', 'C1001', DATE '2025-01-15', 'Overturned'),
('A5002', 'C1002', DATE '2025-01-16', 'Pending'),
('A5003', 'C1003', DATE '2025-01-17', 'Upheld'),
('A5004', 'C1004', DATE '2025-01-18', 'Overturned'),
('A5005', 'C1005', DATE '2025-01-19', 'Pending'),
('A5006', 'C1006', DATE '2025-01-20', 'Upheld'),
('A5007', 'C1007', DATE '2025-01-21', 'Pending'),
('A5008', 'C1008', DATE '2025-01-22', 'Overturned'),
('A5009', 'C1009', DATE '2025-01-23', 'Pending'),
('A5010', 'C1010', DATE '2025-01-24', 'Upheld'),
('A5011', 'C1011', DATE '2025-01-25', 'Overturned'),
('A5012', 'C1012', DATE '2025-01-26', 'Upheld'),
('A5013', 'C1013', DATE '2025-01-27', 'Pending'),
('A5014', 'C1014', DATE '2025-01-28', 'Overturned'),
('A5015', 'C1015', DATE '2025-01-29', 'Pending'),
('A5016', 'C1016', DATE '2025-01-30', 'Upheld'),
('A5017', 'C1017', DATE '2025-01-31', 'Overturned'),
('A5018', 'C1018', DATE '2025-02-01', 'Upheld'),
('A5019', 'C1019', DATE '2025-02-02', 'Pending'),
('A5020', 'C1020', DATE '2025-02-03', 'Overturned'),
('A5021', 'C1023', DATE '2025-02-07', 'Overturned'),
('A5022', 'C1026', DATE '2025-02-10', 'Overturned'),
('A5023', 'C1029', DATE '2025-02-13', 'Overturned'),
('A5024', 'C1032', DATE '2025-02-16', 'Overturned'),
('A5025', 'C1035', DATE '2025-02-19', 'Overturned'),
('A5026', 'C1038', DATE '2025-02-22', 'Overturned'),
('A5027', 'C1041', DATE '2025-02-25', 'Pending'),
('A5028', 'C1044', DATE '2025-02-28', 'Pending'),
('A5029', 'C1047', DATE '2025-03-03', 'Pending'),
('A5030', 'C1050', DATE '2025-03-06', 'Pending');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Appeal Outcomes
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareAppealsDB.Silver.AppealOutcomes AS
SELECT
    d.ClaimID,
    d.Payer,
    d.DenialReason,
    d.DenialAmount,
    a.Outcome
FROM HealthcareAppealsDB.Bronze.Denials d
LEFT JOIN HealthcareAppealsDB.Bronze.Appeals a ON d.ClaimID = a.ClaimID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Strategy Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareAppealsDB.Gold.StrategyReport AS
SELECT
    Payer,
    DenialReason,
    COUNT(ClaimID) AS TotalDenials,
    SUM(CASE WHEN Outcome = 'Overturned' THEN 1 ELSE 0 END) AS Wins,
    SUM(CASE WHEN Outcome = 'Overturned' THEN DenialAmount ELSE 0.0 END) AS RecoveredAmount,
    (CAST(SUM(CASE WHEN Outcome = 'Overturned' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(ClaimID)) * 100 AS WinRatePct
FROM HealthcareAppealsDB.Silver.AppealOutcomes
GROUP BY Payer, DenialReason;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Payer has the highest overturn rate?"
    2. "Calculate total revenue recovered by Denial Reason."
    3. "Show the win rate for 'Medical Necessity' denials."
*/
