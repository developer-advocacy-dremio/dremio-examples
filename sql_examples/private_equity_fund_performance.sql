/*
 * Private Equity Fund Performance
 * 
 * Scenario:
 * Tracking portfolio company valuation, EBITDA, and MOIC (Multiple on Invested Capital).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.PE;
CREATE FOLDER IF NOT EXISTS RetailDB.PE.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.PE.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.PE.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.PE.Bronze.PortfolioCos (
    CompanyID INT,
    Name VARCHAR,
    Fund VARCHAR,
    InitialInvestment DOUBLE,
    CurrentValuation DOUBLE,
    LTM_EBITDA DOUBLE
);

INSERT INTO RetailDB.PE.Bronze.PortfolioCos VALUES
(1, 'AlphaTech', 'Fund III', 10000000, 25000000, 2000000),
(2, 'BetaRetail', 'Fund III', 15000000, 18000000, 4000000),
(3, 'CharlieHealth', 'Fund IV', 20000000, 22000000, 1500000),
(4, 'DeltaLogistics', 'Fund II', 8000000, 32000000, 5000000), -- High performer
(5, 'EchoMedia', 'Fund IV', 12000000, 10000000, 800000),  -- Underweight
(6, 'FoxtrotCyber', 'Fund III', 18000000, 24000000, 1200000),
(7, 'GolfEnergy', 'Fund II', 25000000, 45000000, 9000000),
(8, 'HotelUniforms', 'Fund II', 5000000, 6000000, 1100000),
(9, 'IndiaSoftware', 'Fund IV', 10000000, 14000000, 950000),
(10, 'JulietBio', 'Fund IV', 30000000, 28000000, -500000);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.PE.Silver.PerformanceMetrics AS
SELECT 
    CompanyID,
    Name,
    Fund,
    InitialInvestment,
    CurrentValuation,
    (CurrentValuation / InitialInvestment) AS MOIC, -- Multiple on Invested Capital
    LTM_EBITDA,
    (CurrentValuation / NULLIF(LTM_EBITDA, 0)) AS ValuationMultiple
FROM RetailDB.PE.Bronze.PortfolioCos;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.PE.Gold.FundReturns AS
SELECT 
    Fund,
    SUM(InitialInvestment) AS TotalDeployed,
    SUM(CurrentValuation) AS TotalValue,
    (SUM(CurrentValuation) / SUM(InitialInvestment)) AS Fund_MOIC
FROM RetailDB.PE.Silver.PerformanceMetrics
GROUP BY Fund;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Fund has the highest MOIC in RetailDB.PE.Gold.FundReturns?"
*/
