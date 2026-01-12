/*
 * Asset Management Portfolio Rebalancing
 * 
 * Scenario:
 * Comparing actual portfolio allocations against target weights to identify rebalancing needs.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.AssetMgmt;
CREATE FOLDER IF NOT EXISTS RetailDB.AssetMgmt.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.AssetMgmt.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.AssetMgmt.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.AssetMgmt.Bronze.Portfolios (
    PortfolioID INT,
    AssetClass VARCHAR,
    CurrentValue DOUBLE,
    TargetWeight DOUBLE -- Percentage (e.g., 0.40 for 40%)
);

INSERT INTO RetailDB.AssetMgmt.Bronze.Portfolios VALUES
-- Portfolio 1: Aggressive
(1, 'US Equity', 550000, 0.50),
(1, 'Intl Equity', 320000, 0.30),
(1, 'Bonds', 100000, 0.15),
(1, 'Cash', 30000, 0.05),
-- Portfolio 2: Conservative
(2, 'US Equity', 200000, 0.20),
(2, 'Intl Equity', 100000, 0.10),
(2, 'Bonds', 650000, 0.60),
(2, 'Cash', 150000, 0.10),
-- Portfolio 3: Balanced
(3, 'US Equity', 420000, 0.40),
(3, 'Intl Equity', 200000, 0.20),
(3, 'Bonds', 350000, 0.35),
(3, 'Cash', 50000, 0.05);
-- (12 rows total)

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.AssetMgmt.Silver.AllocationAnalysis AS
SELECT 
    PortfolioID,
    AssetClass,
    CurrentValue,
    TargetWeight,
    SUM(CurrentValue) OVER (PARTITION BY PortfolioID) AS TotalPortfolioValue
FROM RetailDB.AssetMgmt.Bronze.Portfolios;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.AssetMgmt.Gold.RebalanceTriggers AS
SELECT 
    PortfolioID,
    AssetClass,
    CurrentValue,
    (CurrentValue / TotalPortfolioValue) AS ActualWeight,
    TargetWeight,
    ((CurrentValue / TotalPortfolioValue) - TargetWeight) AS Drift,
    CASE 
        WHEN ABS((CurrentValue / TotalPortfolioValue) - TargetWeight) > 0.05 THEN 'REBALANCE REQUIRED'
        ELSE 'On Track'
    END AS Status
FROM RetailDB.AssetMgmt.Silver.AllocationAnalysis;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all asset classes from RetailDB.AssetMgmt.Gold.RebalanceTriggers where Status is 'REBALANCE REQUIRED'."
*/
