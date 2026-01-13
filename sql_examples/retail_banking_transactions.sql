/*
 * Retail Banking Transactions Demo
 * 
 * Scenario:
 * Analyzing branch performance, ATM usage, and account balances to identify high-traffic locations and customer trends.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.RetailBanking;
CREATE FOLDER IF NOT EXISTS RetailDB.RetailBanking.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.RetailBanking.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.RetailBanking.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.RetailBanking.Bronze.Branches (
    BranchID INT,
    BranchName VARCHAR,
    City VARCHAR,
    State VARCHAR,
    ManagerName VARCHAR
);

INSERT INTO RetailDB.RetailBanking.Bronze.Branches VALUES
(1, 'Downtown Main', 'New York', 'NY', 'Alice Smith'),
(2, 'Westside', 'New York', 'NY', 'Bob Jones'),
(3, 'Brooklyn Heights', 'Brooklyn', 'NY', 'Charlie Day'),
(4, 'Queens Plaza', 'Queens', 'NY', 'Diane Ross'),
(5, 'Jersey City Hub', 'Jersey City', 'NJ', 'Evan Wright'),
(6, 'Newark Central', 'Newark', 'NJ', 'Fiona Green'),
(7, 'Stamford North', 'Stamford', 'CT', 'George Hall'),
(8, 'White Plains', 'White Plains', 'NY', 'Hannah Lee'),
(9, 'Bronx South', 'Bronx', 'NY', 'Ian Clark'),
(10, 'Hoboken Terminal', 'Hoboken', 'NJ', 'Jane Doe');

CREATE TABLE IF NOT EXISTS RetailDB.RetailBanking.Bronze.Transactions (
    TxID INT,
    AccountID INT,
    BranchID INT,
    TxType VARCHAR,
    TxAmount DOUBLE,
    TxDate TIMESTAMP
);

INSERT INTO RetailDB.RetailBanking.Bronze.Transactions VALUES
(101, 1001, 1, 'Deposit', 500.00, '2025-01-01 09:00:00'),
(102, 1002, 1, 'Withdrawal', 200.00, '2025-01-01 09:15:00'),
(103, 1003, 2, 'Deposit', 1200.00, '2025-01-01 09:30:00'),
(104, 1004, 3, 'ATM', 50.00, '2025-01-01 09:45:00'),
(105, 1005, 1, 'Deposit', 3000.00, '2025-01-01 10:00:00'),
(106, 1001, 2, 'Transfer', 150.00, '2025-01-01 10:15:00'),
(107, 1006, 4, 'Withdrawal', 100.00, '2025-01-01 10:30:00'),
(108, 1007, 5, 'Deposit', 450.00, '2025-01-01 11:00:00'),
(109, 1008, 1, 'ATM', 20.00, '2025-01-01 11:15:00'),
(110, 1009, 6, 'Deposit', 750.00, '2025-01-01 11:30:00'),
(111, 1010, 7, 'Withdrawal', 300.00, '2025-01-01 11:45:00'),
(112, 1002, 8, 'Deposit', 2000.00, '2025-01-01 12:00:00'),
(113, 1003, 9, 'Transfer', 500.00, '2025-01-01 12:15:00'),
(114, 1011, 10, 'ATM', 100.00, '2025-01-01 12:30:00'),
(115, 1012, 1, 'Deposit', 100.00, '2025-01-01 13:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Enriched Data
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.RetailBanking.Silver.EnrichedTransactions AS
SELECT 
    t.TxID,
    t.AccountID,
    b.BranchName,
    b.City,
    b.State,
    t.TxType,
    t.TxAmount,
    t.TxDate
FROM RetailDB.RetailBanking.Bronze.Transactions t
JOIN RetailDB.RetailBanking.Bronze.Branches b ON t.BranchID = b.BranchID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Aggregated Insights
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.RetailBanking.Gold.BranchPerformance AS
SELECT 
    BranchName,
    State,
    COUNT(*) AS TotalTransactions,
    SUM(CASE WHEN TxType = 'Deposit' THEN TxAmount ELSE 0 END) AS TotalDeposits,
    SUM(CASE WHEN TxType = 'Withdrawal' THEN TxAmount ELSE 0 END) AS TotalWithdrawals
FROM RetailDB.RetailBanking.Silver.EnrichedTransactions
GROUP BY BranchName, State;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT 1:
"Using RetailDB.RetailBanking.Gold.BranchPerformance, give me a list of branches with more than $2000 in total deposits."

PROMPT 2:
"Show the distribution of transaction types for New York branches from the Silver layer."
*/
