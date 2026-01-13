/*
 * Mortgage Prepayment Risk Demo
 * 
 * Scenario:
 * Banks lose interest income when borrowers pay off mortgages early (Prepayment Risk).
 * Triggered by refinancing incentives (rate drops).
 * 
 * Data Context:
 * - Mortgages: Active loans with interest rates.
 * - MarketRates: Current prevailing mortgage rate.
 * 
 * Analytical Goal:
 * Calculate the "Incentive to Refinance" (CurrentRate - MarketRate). High incentive = High Risk.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MortgageRiskDB;
CREATE FOLDER IF NOT EXISTS MortgageRiskDB.Bronze;
CREATE FOLDER IF NOT EXISTS MortgageRiskDB.Silver;
CREATE FOLDER IF NOT EXISTS MortgageRiskDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MortgageRiskDB.Bronze.Portfolio (
    LoanID VARCHAR,
    BorrowerID VARCHAR,
    OriginationDate DATE,
    Balance DOUBLE,
    InterestRate DOUBLE, -- e.g., 6.5
    TermYears INT
);

CREATE TABLE IF NOT EXISTS MortgageRiskDB.Bronze.MarketData (
    "Date" DATE,
    Rate30YearFixed DOUBLE -- e.g., 5.0
);

INSERT INTO MortgageRiskDB.Bronze.Portfolio VALUES
('M-101', 'B-01', '2023-01-01', 400000.0, 7.0, 30), -- High Risk (7.0 vs 5.5)
('M-102', 'B-02', '2023-02-01', 350000.0, 6.8, 30),
('M-103', 'B-03', '2023-03-01', 500000.0, 6.5, 30),
('M-104', 'B-04', '2021-01-01', 200000.0, 3.0, 30), -- Safe (Low Rate)
('M-105', 'B-05', '2021-02-01', 250000.0, 3.2, 30),
('M-106', 'B-06', '2024-01-01', 600000.0, 7.2, 30), -- Very High Risk
('M-107', 'B-07', '2023-05-01', 450000.0, 6.9, 30),
('M-108', 'B-08', '2023-06-01', 300000.0, 6.6, 30),
('M-109', 'B-09', '2022-01-01', 150000.0, 4.0, 15),
('M-110', 'B-10', '2023-11-01', 550000.0, 7.5, 30);

INSERT INTO MortgageRiskDB.Bronze.MarketData VALUES
('2025-10-01', 5.5); -- Current market rate drops to 5.5%

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MortgageRiskDB.Silver.RefiIncentive AS
SELECT 
    p.LoanID,
    p.Balance,
    p.InterestRate AS CurrentRate,
    m.Rate30YearFixed AS MarketRate,
    (p.InterestRate - m.Rate30YearFixed) AS RateDifferential
FROM MortgageRiskDB.Bronze.Portfolio p
CROSS JOIN MortgageRiskDB.Bronze.MarketData m
WHERE m."Date" = '2025-10-01';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MortgageRiskDB.Gold.PrepaymentExposure AS
SELECT 
    LoanID,
    Balance,
    RateDifferential,
    CASE 
        WHEN RateDifferential > 1.0 THEN 'High Risk' -- 100bps incentive
        WHEN RateDifferential > 0.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS ChurnRisk
FROM MortgageRiskDB.Silver.RefiIncentive;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Sum the Balance for all 'High Risk' loans in MortgageRiskDB.Gold.PrepaymentExposure."
*/
