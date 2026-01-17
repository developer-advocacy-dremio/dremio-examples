/*
 * Retail Banking Churn Prediction Demo
 * 
 * Scenario:
 * The retail bank wants to identify customers at risk of leaving (churning).
 * Early warning signs include unexpected drops in login frequency or transaction volume.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Generate a list of high-value "At Risk" customers for retention campaigns.
 * 
 * Note: Assumes a catalog named 'RetailBankDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS RetailBankDB;
CREATE FOLDER IF NOT EXISTS RetailBankDB.Bronze;
CREATE FOLDER IF NOT EXISTS RetailBankDB.Silver;
CREATE FOLDER IF NOT EXISTS RetailBankDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Customer & Activity Data
-------------------------------------------------------------------------------
-- Description: Customer static details and dynamic activity logs.

CREATE TABLE IF NOT EXISTS RetailBankDB.Bronze.CustomerProfiles (
    CustomerID VARCHAR,
    JoinDate DATE,
    Segment VARCHAR, -- 'MassMarket', 'Preferred', 'PrivateBank'
    TotalBalance DOUBLE
);

CREATE TABLE IF NOT EXISTS RetailBankDB.Bronze.AccountActivity (
    LogID VARCHAR,
    CustomerID VARCHAR,
    ActivityType VARCHAR, -- 'Login', 'Transfer', 'ATM', 'BillPay'
    ActivityDate DATE
);

-- 1.1 Populate CustomerProfiles (50 Records)
INSERT INTO RetailBankDB.Bronze.CustomerProfiles (CustomerID, JoinDate, Segment, TotalBalance) VALUES
('C-001', '2020-05-15', 'Preferred', 55000.00),
('C-002', '2019-03-10', 'MassMarket', 2500.00),
('C-003', '2021-11-20', 'MassMarket', 800.00),
('C-004', '2018-06-01', 'PrivateBank', 1250000.00),
('C-005', '2022-01-15', 'MassMarket', 1500.00),
('C-006', '2020-08-22', 'Preferred', 75000.00),
('C-007', '2017-12-05', 'MassMarket', 4000.00),
('C-008', '2023-02-14', 'MassMarket', 500.00),
('C-009', '2016-09-30', 'PrivateBank', 500000.00),
('C-010', '2019-07-04', 'Preferred', 45000.00),
('C-011', '2021-03-12', 'MassMarket', 1200.00),
('C-012', '2020-10-25', 'MassMarket', 3000.00),
('C-013', '2018-04-18', 'Preferred', 60000.00),
('C-014', '2022-06-09', 'MassMarket', 200.00),
('C-015', '2019-12-01', 'PrivateBank', 900000.00),
('C-016', '2020-02-14', 'MassMarket', 2200.00),
('C-017', '2017-05-20', 'MassMarket', 5500.00),
('C-018', '2021-09-08', 'Preferred', 35000.00),
('C-019', '2018-11-11', 'PrivateBank', 250000.00),
('C-020', '2023-01-05', 'MassMarket', 100.00),
('C-021', '2019-06-25', 'Preferred', 52000.00),
('C-022', '2020-04-04', 'MassMarket', 3800.00),
('C-023', '2016-08-15', 'MassMarket', 750.00),
('C-024', '2022-12-12', 'PrivateBank', 750000.00),
('C-025', '2021-05-30', 'MassMarket', 1800.00),
('C-026', '2018-09-19', 'Preferred', 48000.00),
('C-027', '2017-02-28', 'MassMarket', 3200.00),
('C-028', '2020-11-11', 'MassMarket', 900.00),
('C-029', '2019-01-22', 'PrivateBank', 1100000.00),
('C-030', '2022-08-08', 'Preferred', 65000.00),
('C-031', '2021-04-15', 'MassMarket', 2600.00),
('C-032', '2016-10-10', 'MassMarket', 450.00),
('C-033', '2018-03-25', 'Preferred', 80000.00),
('C-034', '2020-07-07', 'PrivateBank', 450000.00),
('C-035', '2023-03-03', 'MassMarket', 600.00),
('C-036', '2017-06-18', 'MassMarket', 5100.00),
('C-037', '2019-09-09', 'Preferred', 42000.00),
('C-038', '2021-12-25', 'MassMarket', 1100.00),
('C-039', '2018-05-05', 'PrivateBank', 300000.00),
('C-040', '2022-02-20', 'MassMarket', 2800.00),
('C-041', '2020-01-01', 'Preferred', 58000.00),
('C-042', '2019-04-14', 'MassMarket', 350.00),
('C-043', '2017-08-30', 'MassMarket', 6200.00),
('C-044', '2021-10-10', 'PrivateBank', 1500000.00),
('C-045', '2018-02-18', 'Preferred', 68000.00),
('C-046', '2022-07-27', 'MassMarket', 1600.00),
('C-047', '2020-06-06', 'MassMarket', 4100.00),
('C-048', '2019-11-21', 'Preferred', 39000.00),
('C-049', '2016-12-12', 'PrivateBank', 600000.00),
('C-050', '2023-01-20', 'MassMarket', 850.00);

-- 1.2 Populate AccountActivity (50+ Records)
INSERT INTO RetailBankDB.Bronze.AccountActivity (LogID, CustomerID, ActivityType, ActivityDate) VALUES
('L-001', 'C-001', 'Login', '2025-01-01'),
('L-002', 'C-001', 'Transfer', '2025-01-02'),
('L-003', 'C-002', 'Login', '2025-01-05'), -- Infrequent
('L-004', 'C-004', 'Login', '2025-01-01'),
('L-005', 'C-004', 'Wire', '2025-01-03'),
('L-006', 'C-004', 'Login', '2025-01-04'),
('L-007', 'C-006', 'ATM', '2025-01-02'),
('L-008', 'C-009', 'Login', '2025-01-01'), -- High Value Active
('L-009', 'C-009', 'BillPay', '2025-01-05'),
('L-010', 'C-034', 'Login', '2024-12-01'), -- Last login > 30 days ago (Churn Risk)
('L-011', 'C-035', 'ATM', '2025-01-10'),
('L-012', 'C-001', 'Login', '2025-01-08'),
('L-013', 'C-005', 'Debit', '2025-01-02'),
('L-014', 'C-005', 'Login', '2025-01-09'),
('L-015', 'C-010', 'Login', '2025-01-03'),
('L-016', 'C-011', 'ATM', '2025-01-05'),
('L-017', 'C-013', 'Wire', '2025-01-06'),
('L-018', 'C-015', 'Login', '2024-11-15'), -- Stale
('L-019', 'C-016', 'Debit', '2025-01-07'),
('L-020', 'C-018', 'Login', '2025-01-01'),
('L-021', 'C-018', 'Login', '2025-01-02'),
('L-022', 'C-018', 'Login', '2025-01-03'),
('L-023', 'C-019', 'Login', '2025-01-05'),
('L-024', 'C-021', 'BillPay', '2025-01-04'),
('L-025', 'C-022', 'ATM', '2025-01-08'),
('L-026', 'C-024', 'Login', '2024-10-01'), -- Very Stale
('L-027', 'C-025', 'Login', '2025-01-09'),
('L-028', 'C-026', 'Debit', '2025-01-01'),
('L-029', 'C-029', 'Wire', '2025-01-10'),
('L-030', 'C-029', 'Login', '2025-01-11'),
('L-031', 'C-030', 'Login', '2025-01-02'),
('L-032', 'C-033', 'Transfer', '2025-01-06'),
('L-033', 'C-037', 'Login', '2025-01-07'),
('L-034', 'C-039', 'Login', '2025-01-04'),
('L-035', 'C-040', 'Debit', '2025-01-03'),
('L-036', 'C-041', 'ATM', '2025-01-08'),
('L-037', 'C-044', 'Login', '2025-01-01'),
('L-038', 'C-044', 'Wire', '2025-01-05'),
('L-039', 'C-045', 'Login', '2025-01-06'),
('L-040', 'C-048', 'BillPay', '2025-01-09'),
('L-041', 'C-049', 'Login', '2025-01-10'),
('L-042', 'C-050', 'Debit', '2025-01-02'),
('L-043', 'C-003', 'Login', '2025-01-03'),
('L-044', 'C-007', 'ATM', '2025-01-04'),
('L-045', 'C-008', 'Login', '2025-01-05'),
('L-046', 'C-012', 'Debit', '2025-01-06'),
('L-047', 'C-014', 'Login', '2025-01-07'),
('L-048', 'C-017', 'Wire', '2025-01-08'),
('L-049', 'C-020', 'ATM', '2025-01-09'),
('L-050', 'C-023', 'Login', '2025-01-10');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Behavioral Features
-------------------------------------------------------------------------------
-- Description: Calculating "Days Since Last Active" and frequency metrics.
-- Transformation: Group by Customer to find MAX(ActivityDate).

CREATE OR REPLACE VIEW RetailBankDB.Silver.ActivityTrends AS
SELECT
    p.CustomerID,
    p.Segment,
    p.TotalBalance,
    MAX(a.ActivityDate) AS LastActiveDate,
    DATEDIFF(day, MAX(a.ActivityDate), DATE '2025-01-15') AS DaysSinceLastActive, -- Simulating 'today' as Jan 15th
    COUNT(a.LogID) AS ActivityCount30Days
FROM RetailBankDB.Bronze.CustomerProfiles p
LEFT JOIN RetailBankDB.Bronze.AccountActivity a ON p.CustomerID = a.CustomerID
GROUP BY p.CustomerID, p.Segment, p.TotalBalance;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Retention Action List
-------------------------------------------------------------------------------
-- Description: Identifying high-value customers who haven't been active in > 30 days.
-- Insight: Prioritizing "Preferred" and "PrivateBank" segments for outreach.

CREATE OR REPLACE VIEW RetailBankDB.Gold.HighRiskChurnCandidates AS
SELECT
    CustomerID,
    Segment,
    TotalBalance,
    DaysSinceLastActive,
    'Urgent' AS OutreachPriority
FROM RetailBankDB.Silver.ActivityTrends
WHERE DaysSinceLastActive > 30 
  AND TotalBalance > 10000;

CREATE OR REPLACE VIEW RetailBankDB.Gold.ChurnRiskSummary AS
SELECT
    Segment,
    COUNT(CustomerID) AS AtRiskCount,
    SUM(TotalBalance) AS AtRiskAUM
FROM RetailBankDB.Gold.HighRiskChurnCandidates
GROUP BY Segment;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all HighRiskChurnCandidates in the 'PrivateBank' segment, ordered by TotalBalance descending."

PROMPT:
"Visualize the AtRiskAUM by Segment from RetailBankDB.Gold.ChurnRiskSummary using a bar chart."
*/
