/*
 * AML Structuring Detection Demo
 * 
 * Scenario:
 * Detecting "Smurfing" (Structuring) - where large transactions are broken down into smaller ones 
 * to evade regulatory reporting thresholds (e.g., $10,000 USD).
 * 
 * Data Context:
 * - Transactions: Raw cash deposit data from bank branches.
 * - Customers: KYC details including risk profile.
 * 
 * Analytical Goal:
 * Identify customers with multiple cash deposits just below the $10k reporting limit within a short time window (e.g., 24-48 hours).
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS AML_RiskDB;
CREATE FOLDER IF NOT EXISTS AML_RiskDB.Bronze;
CREATE FOLDER IF NOT EXISTS AML_RiskDB.Silver;
CREATE FOLDER IF NOT EXISTS AML_RiskDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AML_RiskDB.Bronze.Transactions (
    TxnID VARCHAR,
    CustomerID VARCHAR,
    BranchID VARCHAR,
    "Date" DATE,
    "Timestamp" TIMESTAMP,
    Amount DOUBLE,
    Currency VARCHAR,
    TxnType VARCHAR -- 'Deposit', 'Withdrawal', 'Transfer'
);

CREATE TABLE IF NOT EXISTS AML_RiskDB.Bronze.Customers (
    CustomerID VARCHAR,
    FullName VARCHAR,
    RiskRating VARCHAR, -- 'Low', 'Medium', 'High'
    Country VARCHAR
);

-- Insert 10+ records for Transactions
INSERT INTO AML_RiskDB.Bronze.Transactions VALUES
('T-1001', 'C-500', 'B-01', '2025-08-01', '2025-08-01 09:15:00', 9500.00, 'USD', 'Deposit'), -- Suspicious
('T-1002', 'C-500', 'B-02', '2025-08-01', '2025-08-01 11:30:00', 9000.00, 'USD', 'Deposit'), -- Suspicious
('T-1003', 'C-500', 'B-01', '2025-08-02', '2025-08-02 09:00:00', 9800.00, 'USD', 'Deposit'), -- Suspicious
('T-1004', 'C-501', 'B-03', '2025-08-01', '2025-08-01 10:00:00', 200.00, 'USD', 'Deposit'),
('T-1005', 'C-501', 'B-03', '2025-08-01', '2025-08-01 14:00:00', 500.00, 'USD', 'Withdrawal'),
('T-1006', 'C-502', 'B-02', '2025-08-01', '2025-08-01 15:00:00', 12000.00, 'USD', 'Deposit'), -- Above threshold (CTR filed automatically)
('T-1007', 'C-503', 'B-01', '2025-08-03', '2025-08-03 10:00:00', 5000.00, 'USD', 'Deposit'),
('T-1008', 'C-503', 'B-01', '2025-08-03', '2025-08-03 16:00:00', 2000.00, 'USD', 'Deposit'),
('T-1009', 'C-500', 'B-04', '2025-08-04', '2025-08-04 09:15:00', 9900.00, 'USD', 'Deposit'), -- Suspicious pattern continues
('T-1010', 'C-504', 'B-05', '2025-08-01', '2025-08-01 12:00:00', 50.00, 'USD', 'Transfer');

-- Insert 5 records for Customers
INSERT INTO AML_RiskDB.Bronze.Customers VALUES
('C-500', 'John Doe', 'High', 'USA'),
('C-501', 'Jane Smith', 'Low', 'USA'),
('C-502', 'ACME Corp', 'Medium', 'USA'),
('C-503', 'Robert Brown', 'Low', 'USA'),
('C-504', 'Alice Green', 'Low', 'USA');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AML_RiskDB.Silver.DailyCustomerDeposits AS
SELECT 
    t.CustomerID,
    c.FullName,
    c.RiskRating,
    t."Date",
    COUNT(t.TxnID) AS DepositCount,
    SUM(t.Amount) AS TotalDepositAmount,
    MAX(t.Amount) AS MaxSingleDeposit
FROM AML_RiskDB.Bronze.Transactions t
JOIN AML_RiskDB.Bronze.Customers c ON t.CustomerID = c.CustomerID
WHERE t.TxnType = 'Deposit'
GROUP BY t.CustomerID, c.FullName, c.RiskRating, t."Date";

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

-- Identify Potential Structuring:
-- Customers who deposited < $10k in single transactions but > $10k total in one day.
CREATE OR REPLACE VIEW AML_RiskDB.Gold.StructuringAlerts AS
SELECT 
    CustomerID,
    FullName,
    RiskRating,
    "Date",
    DepositCount,
    TotalDepositAmount,
    'Potential Structuring' AS AlertType
FROM AML_RiskDB.Silver.DailyCustomerDeposits
WHERE TotalDepositAmount > 10000 
  AND MaxSingleDeposit < 10000;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Visualize the TotalDepositAmount by FullName from AML_RiskDB.Gold.StructuringAlerts as a bar chart to spot high-risk structuring behavior."

PROMPT:
"List all StructuringAlerts where the RiskRating is 'High' and sort by TotalDepositAmount descending."
*/
