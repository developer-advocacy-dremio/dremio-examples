/*
    Dremio High-Volume SQL Pattern: Government Grant Management
    
    Business Scenario:
    Agencies win Federal/State grants for specific projects. Tracking "Burn Rate" and "Compliance"
    is required to avoid returning funds or facing audit penalties.
    
    Data Story:
    We track Grant Awards and Spending Transactions.
    
    Medallion Architecture:
    - Bronze: Grants, Expenses.
      *Volume*: 50+ records.
    - Silver: GrantFinancials (Award vs Spend).
    - Gold: RiskReport (Low burn rate or near expiry).
    
    Key Dremio Features:
    - Date Math (Days Remaining)
    - Division for % utilization
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentGrantsDB;
CREATE FOLDER IF NOT EXISTS GovernmentGrantsDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentGrantsDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentGrantsDB.Gold;
USE GovernmentGrantsDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentGrantsDB.Bronze.Grants (
    GrantID STRING,
    ProjectName STRING,
    FundingSource STRING, -- Federal, State, Private
    AwardAmount DOUBLE,
    StartDate DATE,
    EndDate DATE
);

INSERT INTO GovernmentGrantsDB.Bronze.Grants VALUES
('G1', 'Clean Energy Init', 'Federal', 500000.0, DATE '2024-01-01', DATE '2025-12-31'),
('G2', 'Digital Literacy', 'State', 100000.0, DATE '2024-06-01', DATE '2025-06-01'),
('G3', 'Youth Sports', 'Private', 50000.0, DATE '2025-01-01', DATE '2025-12-31'),
('G4', 'Rural Broadband', 'Federal', 2000000.0, DATE '2023-01-01', DATE '2026-01-01'),
('G5', 'Senior Care', 'State', 75000.0, DATE '2024-09-01', DATE '2025-09-01');

CREATE OR REPLACE TABLE GovernmentGrantsDB.Bronze.Expenses (
    ExpenseID STRING,
    GrantID STRING,
    Date DATE,
    Amount DOUBLE,
    Category STRING -- Personnel, Equipment, Travel
);

-- Bulk Expenses (50 Records)
INSERT INTO GovernmentGrantsDB.Bronze.Expenses VALUES
('E1001', 'G1', DATE '2024-01-15', 5000.0, 'Personnel'),
('E1002', 'G1', DATE '2024-02-15', 5000.0, 'Personnel'),
('E1003', 'G1', DATE '2024-03-15', 5000.0, 'Personnel'),
('E1004', 'G1', DATE '2024-04-01', 15000.0, 'Equipment'),
('E1005', 'G2', DATE '2024-06-10', 2000.0, 'Travel'),
('E1006', 'G2', DATE '2024-07-15', 5000.0, 'Personnel'),
('E1007', 'G3', DATE '2025-01-05', 1000.0, 'Equipment'),
('E1008', 'G4', DATE '2023-02-01', 50000.0, 'Equipment'),
('E1009', 'G4', DATE '2023-03-01', 10000.0, 'Personnel'),
('E1010', 'G1', DATE '2024-05-15', 5000.0, 'Personnel'),
('E1011', 'G1', DATE '2024-05-20', 25000.0, 'Equipment'),
('E1012', 'G2', DATE '2024-08-01', 3000.0, 'Travel'),
('E1013', 'G2', DATE '2024-09-01', 5000.0, 'Personnel'),
('E1014', 'G3', DATE '2025-01-15', 2000.0, 'Personnel'),
('E1015', 'G5', DATE '2024-10-01', 1500.0, 'Travel'),
('E1016', 'G4', DATE '2023-06-01', 100000.0, 'Equipment'),
('E1017', 'G4', DATE '2023-09-01', 12000.0, 'Personnel'),
('E1018', 'G1', DATE '2024-06-15', 5000.0, 'Personnel'),
('E1019', 'G1', DATE '2024-07-15', 5000.0, 'Personnel'),
('E1020', 'G2', DATE '2024-10-01', 4000.0, 'Equipment'),
('E1021', 'G3', DATE '2025-02-01', 1000.0, 'Travel'),
('E1022', 'G5', DATE '2024-11-01', 5000.0, 'Personnel'),
('E1023', 'G4', DATE '2024-01-01', 200000.0, 'Equipment'),
('E1024', 'G1', DATE '2024-08-15', 5000.0, 'Personnel'),
('E1025', 'G1', DATE '2024-09-15', 5000.0, 'Personnel'),
('E1026', 'G1', DATE '2024-10-01', 50000.0, 'Equipment'),
('E1027', 'G2', DATE '2024-11-15', 5000.0, 'Personnel'),
('E1028', 'G2', DATE '2024-12-01', 2500.0, 'Travel'),
('E1029', 'G3', DATE '2025-02-15', 3000.0, 'Personnel'),
('E1030', 'G5', DATE '2024-12-01', 6000.0, 'Equipment'),
('E1031', 'G4', DATE '2024-06-01', 50000.0, 'Personnel'),
('E1032', 'G1', DATE '2024-11-15', 5000.0, 'Personnel'),
('E1033', 'G1', DATE '2024-12-15', 5000.0, 'Personnel'),
('E1034', 'G2', DATE '2025-01-01', 5000.0, 'Personnel'),
('E1035', 'G3', DATE '2025-03-01', 500.0, 'Travel'),
('E1036', 'G5', DATE '2025-01-01', 5000.0, 'Personnel'),
('E1037', 'G4', DATE '2024-12-01', 150000.0, 'Equipment'),
('E1038', 'G1', DATE '2025-01-15', 5000.0, 'Personnel'),
('E1039', 'G2', DATE '2025-02-01', 3000.0, 'Equipment'),
('E1040', 'G3', DATE '2025-03-15', 2000.0, 'Personnel'),
('E1041', 'G5', DATE '2025-02-01', 2000.0, 'Travel'),
('E1042', 'G4', DATE '2025-01-01', 20000.0, 'Personnel'),
('E1043', 'G1', DATE '2025-02-15', 5000.0, 'Personnel'),
('E1044', 'G2', DATE '2025-03-01', 5000.0, 'Personnel'),
('E1045', 'G3', DATE '2025-04-01', 1500.0, 'Equipment'),
('E1046', 'G5', DATE '2025-03-01', 5000.0, 'Personnel'),
('E1047', 'G4', DATE '2025-02-01', 60000.0, 'Equipment'),
('E1048', 'G1', DATE '2025-03-15', 5000.0, 'Personnel'),
('E1049', 'G2', DATE '2025-04-01', 4000.0, 'Travel'),
('E1050', 'G5', DATE '2025-04-01', 3000.0, 'Equipment');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Financial Status
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentGrantsDB.Silver.GrantPerformance AS
SELECT
    g.GrantID,
    g.ProjectName,
    g.AwardAmount,
    g.EndDate,
    COALESCE(SUM(e.Amount), 0) AS TotalSpent,
    (g.AwardAmount - COALESCE(SUM(e.Amount), 0)) AS RemainingFunds,
    TIMESTAMPDIFF(DAY, DATE '2025-01-20', g.EndDate) AS DaysRemaining
FROM GovernmentGrantsDB.Bronze.Grants g
LEFT JOIN GovernmentGrantsDB.Bronze.Expenses e ON g.GrantID = e.GrantID
GROUP BY g.GrantID, g.ProjectName, g.AwardAmount, g.EndDate;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Burn Rate Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentGrantsDB.Gold.GrantHealth AS
SELECT
    ProjectName,
    (TotalSpent / AwardAmount) * 100 AS UtilizationPct,
    DaysRemaining,
    CASE 
        WHEN DaysRemaining < 90 AND (TotalSpent / AwardAmount) < 0.5 THEN 'RISK_UNDERSPEND'
        WHEN (TotalSpent / AwardAmount) > 0.9 AND DaysRemaining > 180 THEN 'RISK_OVERSPEND'
        ELSE 'ON_TRACK'
    END AS Status
FROM GovernmentGrantsDB.Silver.GrantPerformance;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me grants at risk of underspending."
    2. "Calculate total spending by Grant ID."
    3. "Which project has the highest utilization percentage?"
*/
