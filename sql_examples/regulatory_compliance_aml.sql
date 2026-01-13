/*
 * Regulatory Compliance / AML Demo
 * 
 * Scenario:
 * Detecting money laundering patterns such as "structuring" (smurfing) and large cash transactions.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Compliance;
CREATE FOLDER IF NOT EXISTS RetailDB.Compliance.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Compliance.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Compliance.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Compliance.Bronze.CashTransactions (
    TxID INT,
    AccountID INT,
    Amount DOUBLE,
    TxDate TIMESTAMP,
    Location VARCHAR
);

INSERT INTO RetailDB.Compliance.Bronze.CashTransactions VALUES
(1, 101, 9500.00, '2025-01-01 10:00:00', 'Branch A'), -- Just under reporting limit
(2, 101, 9000.00, '2025-01-02 11:00:00', 'Branch B'), -- Structuring pattern?
(3, 101, 5000.00, '2025-01-02 14:00:00', 'Branch C'),
(4, 102, 200.00, '2025-01-01 09:00:00', 'ATM'),
(5, 103, 15000.00, '2025-01-03 10:00:00', 'Branch A'), -- CTR Trigger
(6, 104, 3000.00, '2025-01-01 12:00:00', 'Branch A'),
(7, 104, 3000.00, '2025-01-01 12:10:00', 'Branch B'),
(8, 104, 3000.00, '2025-01-01 12:20:00', 'Branch C'), -- Velocity
(9, 105, 500.00, '2025-01-04 10:00:00', 'ATM'),
(10, 106, 9900.00, '2025-01-05 15:00:00', 'Branch D');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Compliance.Silver.DailyTotals AS
SELECT 
    AccountID,
    CAST(TxDate AS DATE) AS TxDay,
    COUNT(*) AS TxCount,
    SUM(Amount) AS TotalCash
FROM RetailDB.Compliance.Bronze.CashTransactions
GROUP BY AccountID, CAST(TxDate AS DATE);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Compliance.Gold.SuspiciousActivityReports AS
SELECT 
    AccountID,
    TxDay,
    TotalCash,
    TxCount,
    CASE 
        WHEN TotalCash > 10000 THEN 'CTR Required'
        WHEN TotalCash BETWEEN 9000 AND 10000 AND TxCount > 1 THEN 'Potential Structuring'
        ELSE 'Normal'
    END AS RiskFlag
FROM RetailDB.Compliance.Silver.DailyTotals
WHERE TotalCash > 9000;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Identify all accounts in RetailDB.Compliance.Gold.SuspiciousActivityReports marked as 'Potential Structuring'."
*/
