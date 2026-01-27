/*
 * Mortgage Loan Portfolio Demo
 * 
 * Scenario:
 * Analyzing loan-to-value (LTV) ratios and delinquency risks across a mortgage portfolio.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS FinanceDB;
CREATE FOLDER IF NOT EXISTS FinanceDB.Mortgage;
CREATE FOLDER IF NOT EXISTS FinanceDB.Mortgage.Bronze;
CREATE FOLDER IF NOT EXISTS FinanceDB.Mortgage.Silver;
CREATE FOLDER IF NOT EXISTS FinanceDB.Mortgage.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FinanceDB.Mortgage.Bronze.Loans (
    LoanID INT,
    BorrowerID INT,
    PropertyState VARCHAR,
    LoanAmount DOUBLE,
    AppraisedValue DOUBLE,
    CreditScore INT,
    Status VARCHAR
);

INSERT INTO FinanceDB.Mortgage.Bronze.Loans VALUES
(1, 101, 'TX', 250000, 300000, 720, 'Current'),
(2, 102, 'TX', 400000, 420000, 680, 'Current'),
(3, 103, 'FL', 150000, 200000, 750, 'Current'),
(4, 104, 'FL', 350000, 360000, 620, 'Delinquent-30'),
(5, 105, 'NY', 600000, 800000, 780, 'Current'),
(6, 106, 'NY', 500000, 500000, 650, 'Delinquent-60'),
(7, 107, 'CA', 750000, 900000, 710, 'Current'),
(8, 108, 'CA', 850000, 900000, 690, 'Current'),
(9, 109, 'IL', 200000, 250000, 740, 'Current'),
(10, 110, 'IL', 220000, 240000, 660, 'Delinquent-90'),
(11, 111, 'OH', 120000, 150000, 700, 'Current'),
(12, 112, 'WA', 550000, 600000, 725, 'Current');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FinanceDB.Mortgage.Silver.LoanMetrics AS
SELECT 
    LoanID,
    PropertyState,
    LoanAmount,
    AppraisedValue,
    (LoanAmount / AppraisedValue) * 100 AS LTV_Ratio,
    CreditScore,
    Status
FROM FinanceDB.Mortgage.Bronze.Loans;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FinanceDB.Mortgage.Gold.RiskAnalysis AS
SELECT 
    PropertyState,
    AVG(LTV_Ratio) AS Avg_LTV,
    COUNT(CASE WHEN Status LIKE 'Delinquent%' THEN 1 END) AS DelinquencyCount,
    AVG(CreditScore) AS Avg_CreditScore
FROM FinanceDB.Mortgage.Silver.LoanMetrics
GROUP BY PropertyState;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List states from FinanceDB.Mortgage.Gold.RiskAnalysis that have an Average LTV greater than 80%."
*/
