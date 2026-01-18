/*
    Dremio High-Volume SQL Pattern: Government Pension Liability
    
    Business Scenario:
    Public Sector pension funds (Teachers, Police, Fire) must maintain a healthy "Funding Ratio"
    (Assets / Liabilities). Identifying "Unfunded Liability" helps in fiscal planning.
    
    Data Story:
    We track Members (Active/Retired) and Actuarial Tables.
    
    Medallion Architecture:
    - Bronze: Members, FundAssets.
      *Volume*: 50+ records.
    - Silver: LiabilityCalc (Projected payout stream).
    - Gold: SolvencyReport (Funding Ratio per Plan).
    
    Key Dremio Features:
    - SUM aggregations
    - Ratio arithmetic
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentPensionDB;
CREATE FOLDER IF NOT EXISTS GovernmentPensionDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentPensionDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentPensionDB.Gold;
USE GovernmentPensionDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentPensionDB.Bronze.FundAssets (
    PlanID STRING,
    PlanName STRING, -- Police, Fire, Teachers
    TotalAssets DOUBLE
);

INSERT INTO GovernmentPensionDB.Bronze.FundAssets VALUES
('P1', 'Police & Fire', 150000000.0), -- 150M active correction
('P2', 'Teachers Ret', 200000000.0),  -- 200M
('P3', 'General Civil', 80000000.0);  -- 80M

CREATE OR REPLACE TABLE GovernmentPensionDB.Bronze.Members (
    MemberID STRING,
    PlanID STRING,
    Status STRING, -- Active, Retired
    AnnualPension DOUBLE, -- 0 if Active (future liability logic simplified here)
    YearsOfService INT,
    Age INT
);

-- Bulk Members (50 Records)
INSERT INTO GovernmentPensionDB.Bronze.Members VALUES
('M1001', 'P1', 'Active', 0.0, 10, 35),
('M1002', 'P1', 'Active', 0.0, 15, 40),
('M1003', 'P1', 'Active', 0.0, 20, 45),
('M1004', 'P1', 'Retired', 60000.0, 25, 60),
('M1005', 'P1', 'Retired', 65000.0, 30, 65),
('M1006', 'P1', 'Retired', 55000.0, 25, 70),
('M1007', 'P2', 'Active', 0.0, 5, 28),
('M1008', 'P2', 'Active', 0.0, 8, 32),
('M1009', 'P2', 'Active', 0.0, 12, 36),
('M1010', 'P2', 'Active', 0.0, 20, 48),
('M1011', 'P2', 'Retired', 50000.0, 25, 62),
('M1012', 'P2', 'Retired', 52000.0, 30, 68),
('M1013', 'P3', 'Active', 0.0, 3, 25),
('M1014', 'P3', 'Active', 0.0, 10, 38),
('M1015', 'P3', 'Retired', 40000.0, 20, 66),
('M1016', 'P1', 'Active', 0.0, 5, 30),
('M1017', 'P1', 'Active', 0.0, 18, 42),
('M1018', 'P1', 'Retired', 70000.0, 35, 75),
('M1019', 'P1', 'Retired', 62000.0, 28, 63),
('M1020', 'P2', 'Active', 0.0, 15, 39),
('M1021', 'P2', 'Active', 0.0, 22, 50),
('M1022', 'P2', 'Retired', 48000.0, 24, 61),
('M1023', 'P2', 'Retired', 56000.0, 32, 70),
('M1024', 'P3', 'Active', 0.0, 15, 45),
('M1025', 'P3', 'Retired', 45000.0, 22, 69),
('M1026', 'P1', 'Active', 0.0, 2, 25),
('M1027', 'P1', 'Active', 0.0, 7, 33),
('M1028', 'P1', 'Retired', 58000.0, 24, 67),
('M1029', 'P2', 'Active', 0.0, 4, 27),
('M1030', 'P2', 'Retired', 53000.0, 29, 72),
('M1031', 'P3', 'Active', 0.0, 25, 55),
('M1032', 'P3', 'Retired', 42000.0, 21, 64),
('M1033', 'P1', 'Active', 0.0, 12, 38),
('M1034', 'P1', 'Retired', 68000.0, 33, 71),
('M1035', 'P2', 'Active', 0.0, 10, 34),
('M1036', 'P2', 'Retired', 49000.0, 26, 65),
('M1037', 'P3', 'Active', 0.0, 8, 35),
('M1038', 'P3', 'Retired', 38000.0, 18, 70),
('M1039', 'P1', 'Active', 0.0, 14, 40),
('M1040', 'P2', 'Active', 0.0, 16, 42),
('M1041', 'P3', 'Active', 0.0, 30, 60), -- About to retire
('M1042', 'P1', 'Retired', 72000.0, 38, 78),
('M1043', 'P2', 'Retired', 54000.0, 28, 74),
('M1044', 'P1', 'Active', 0.0, 1, 22),
('M1045', 'P1', 'Active', 0.0, 21, 47),
('M1046', 'P2', 'Active', 0.0, 9, 33),
('M1047', 'P2', 'Retired', 51000.0, 27, 66),
('M1048', 'P3', 'Active', 0.0, 5, 29),
('M1049', 'P3', 'Retired', 46000.0, 23, 72),
('M1050', 'P1', 'Retired', 64000.0, 29, 69);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Liability Estimation
-------------------------------------------------------------------------------
-- Simplified Actuarial logic: 
-- Retired Liability = AnnualPension * 15 (years expectancy)
-- Active Liability = YearsOfService * 5000 (accrued value)
CREATE OR REPLACE VIEW GovernmentPensionDB.Silver.MemberLiability AS
SELECT
    MemberID,
    PlanID,
    Status,
    CASE 
        WHEN Status = 'Retired' THEN AnnualPension * 15
        ELSE YearsOfService * 5000
    END AS EstLiability
FROM GovernmentPensionDB.Bronze.Members;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Funding Ratio
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentPensionDB.Gold.SolvencyStatus AS
SELECT
    a.PlanName,
    a.TotalAssets,
    SUM(l.EstLiability) AS TotalLiability,
    (a.TotalAssets / SUM(l.EstLiability)) * 100 AS FundingRatioPct,
    (a.TotalAssets - SUM(l.EstLiability)) AS UnfundedLiability
FROM GovernmentPensionDB.Bronze.FundAssets a
JOIN GovernmentPensionDB.Silver.MemberLiability l ON a.PlanID = l.PlanID
GROUP BY a.PlanName, a.TotalAssets;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which pension plan has the lowest funding ratio?"
    2. "Show the total liability for Teachers Retirement."
    3. "List all Active members in the Police & Fire plan."
*/
