/*
 * Credit Scoring Model Demo
 * 
 * Scenario:
 * Calculating a FICO-like credit score based on payment history, utilization, and account age.
 * 
 * Data Context:
 * - TradeLines: Individual credit accounts (CC, Mortgage, Auto).
 * - Inquiries: Hard pulls on the credit file.
 * 
 * Analytical Goal:
 * categorize customers into Tier 1 (Prime) to Tier 4 (Subprime) for lending decisions.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS CreditBureauDB;
CREATE FOLDER IF NOT EXISTS CreditBureauDB.Bronze;
CREATE FOLDER IF NOT EXISTS CreditBureauDB.Silver;
CREATE FOLDER IF NOT EXISTS CreditBureauDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CreditBureauDB.Bronze.TradeLines (
    TradeID VARCHAR,
    CustomerID VARCHAR,
    Type VARCHAR, -- 'Revolving', 'Installment'
    CreditLimit DOUBLE,
    CurrentBalance DOUBLE,
    Status VARCHAR, -- 'Current', '30-Late', '90-Late', 'ChargeOff'
    OpenDate DATE
);

CREATE TABLE IF NOT EXISTS CreditBureauDB.Bronze.Inquiries (
    InquiryID VARCHAR,
    CustomerID VARCHAR,
    Date DATE,
    Sector VARCHAR -- 'Auto', 'Mortgage', 'CreditCard'
);

-- 10+ TradeLines
INSERT INTO CreditBureauDB.Bronze.TradeLines VALUES
('T-01', 'C-100', 'Revolving', 10000.0, 500.0, 'Current', '2015-01-01'), -- Good
('T-02', 'C-100', 'Installment', 50000.0, 10000.0, 'Current', '2018-05-01'),
('T-03', 'C-101', 'Revolving', 5000.0, 4900.0, 'Current', '2023-01-01'), -- High Util
('T-04', 'C-101', 'Installment', 20000.0, 19000.0, '30-Late', '2024-01-01'),
('T-05', 'C-102', 'Revolving', 500.0, 0.0, 'Current', '2020-01-01'),
('T-06', 'C-102', 'Revolving', 1000.0, 0.0, 'Current', '2019-01-01'),
('T-07', 'C-103', 'Installment', 25000.0, 0.0, 'ChargeOff', '2010-01-01'), -- Bad
('T-08', 'C-103', 'Revolving', 2000.0, 1500.0, '90-Late', '2022-01-01'),
('T-09', 'C-100', 'Revolving', 15000.0, 200.0, 'Current', '2012-01-01'),
('T-10', 'C-104', 'Revolving', 8000.0, 1000.0, 'Current', '2021-06-01'),
('T-11', 'C-104', 'Installment', 15000.0, 14000.0, 'Current', '2024-01-01'),
('T-12', 'C-105', 'Revolving', 5000.0, 2500.0, 'Current', '2023-01-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CreditBureauDB.Silver.FeatureVector AS
SELECT 
    CustomerID,
    SUM(CurrentBalance) AS TotalDebt,
    SUM(CreditLimit) AS TotalLimit,
    SUM(CurrentBalance) / NULLIF(SUM(CASE WHEN Type = 'Revolving' THEN CreditLimit ELSE 0 END), 0) AS RevolvingUtil,
    COUNT(CASE WHEN Status LIKE '%Late%' OR Status = 'ChargeOff' THEN 1 END) AS DerogatoryCount,
    AVG(TIMESTAMPDIFF(YEAR, OpenDate, CURRENT_DATE)) AS AvgAccountAgeYears
FROM CreditBureauDB.Bronze.TradeLines
GROUP BY CustomerID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

-- Simple Scoring Logic:
-- Base: 600
-- +50 for Low Util (<30%)
-- +50 for Age > 5 Years
-- -100 for any Derogatory
CREATE OR REPLACE VIEW CreditBureauDB.Gold.CalculatedScores AS
SELECT 
    CustomerID,
    RevolvingUtil,
    DerogatoryCount,
    AvgAccountAgeYears,
    (600 + 
     (CASE WHEN RevolvingUtil < 0.3 THEN 50 ELSE 0 END) +
     (CASE WHEN AvgAccountAgeYears > 5 THEN 50 ELSE 0 END) -
     (CASE WHEN DerogatoryCount > 0 THEN 100 ELSE 0 END)
    ) AS EstimatedScore,
    CASE 
        WHEN (600 + (CASE WHEN RevolvingUtil < 0.3 THEN 50 ELSE 0 END) + (CASE WHEN AvgAccountAgeYears > 5 THEN 50 ELSE 0 END) - (CASE WHEN DerogatoryCount > 0 THEN 100 ELSE 0 END)) >= 700 THEN 'Tier 1'
        WHEN (600 + (CASE WHEN RevolvingUtil < 0.3 THEN 50 ELSE 0 END) + (CASE WHEN AvgAccountAgeYears > 5 THEN 50 ELSE 0 END) - (CASE WHEN DerogatoryCount > 0 THEN 100 ELSE 0 END)) >= 600 THEN 'Tier 2'
        ELSE 'Tier 3'
    END AS CreditTier
FROM CreditBureauDB.Silver.FeatureVector;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Count how many customers are in each CreditTier from CreditBureauDB.Gold.CalculatedScores."

PROMPT:
"List all customers with a DerogatoryCount > 0 and their EstimatedScore."
*/
