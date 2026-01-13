/*
 * Robo Advisor Rebalancing Demo
 * 
 * Scenario:
 * Automated portfolio management triggers rebalancing when asset allocation drifts 
 * beyond a tolerance threshold (e.g., +/- 5%).
 * 
 * Data Context:
 * - TargetAlloc: Model portfolio weights (60-40 Stock/Bond).
 * - CurrentHoldings: Customer's actual current weights triggered by market moves.
 * 
 * Analytical Goal:
 * Generate "Buy" and "Sell" signals to restore target weights.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RoboAdvisorDB;
CREATE FOLDER IF NOT EXISTS RoboAdvisorDB.Bronze;
CREATE FOLDER IF NOT EXISTS RoboAdvisorDB.Silver;
CREATE FOLDER IF NOT EXISTS RoboAdvisorDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RoboAdvisorDB.Bronze.TargetModels (
    ModelID VARCHAR, -- 'Aggressive', 'Conservative'
    AssetClass VARCHAR, -- 'Equity', 'FixedIncome', 'Cash'
    TargetWeight DOUBLE -- 0.60, 0.40
);

CREATE TABLE IF NOT EXISTS RoboAdvisorDB.Bronze.Portfolios (
    AccountID VARCHAR,
    ModelID VARCHAR,
    AssetClass VARCHAR,
    CurrentValue DOUBLE
);

INSERT INTO RoboAdvisorDB.Bronze.TargetModels VALUES
('Aggressive', 'Equity', 0.80),
('Aggressive', 'FixedIncome', 0.15),
('Aggressive', 'Cash', 0.05),
('Balanced', 'Equity', 0.50),
('Balanced', 'FixedIncome', 0.40),
('Balanced', 'Cash', 0.10);

INSERT INTO RoboAdvisorDB.Bronze.Portfolios VALUES
('ACC-001', 'Aggressive', 'Equity', 85000.0), -- Drifty (85%)
('ACC-001', 'Aggressive', 'FixedIncome', 10000.0),
('ACC-001', 'Aggressive', 'Cash', 5000.0),
('ACC-002', 'Balanced', 'Equity', 48000.0), -- OK
('ACC-002', 'Balanced', 'FixedIncome', 42000.0),
('ACC-002', 'Balanced', 'Cash', 10000.0),
('ACC-003', 'Aggressive', 'Equity', 70000.0), -- Low
('ACC-003', 'Aggressive', 'FixedIncome', 25000.0), -- High
('ACC-003', 'Aggressive', 'Cash', 5000.0),
('ACC-004', 'Balanced', 'Equity', 50000.0),
('ACC-004', 'Balanced', 'FixedIncome', 40000.0),
('ACC-004', 'Balanced', 'Cash', 10000.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RoboAdvisorDB.Silver.DriftAnalysis AS
SELECT 
    p.AccountID,
    p.ModelID,
    p.AssetClass,
    p.CurrentValue,
    SUM(p.CurrentValue) OVER (PARTITION BY p.AccountID) AS TotalAccountValue,
    (p.CurrentValue / SUM(p.CurrentValue) OVER (PARTITION BY p.AccountID)) AS CurrentWeight,
    t.TargetWeight,
    ((p.CurrentValue / SUM(p.CurrentValue) OVER (PARTITION BY p.AccountID)) - t.TargetWeight) AS Drift
FROM RoboAdvisorDB.Bronze.Portfolios p
JOIN RoboAdvisorDB.Bronze.TargetModels t 
    ON p.ModelID = t.ModelID AND p.AssetClass = t.AssetClass;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RoboAdvisorDB.Gold.RebalanceSignals AS
SELECT 
    AccountID,
    AssetClass,
    CurrentWeight,
    TargetWeight,
    Drift,
    CASE 
        WHEN Drift > 0.05 THEN 'Sell'
        WHEN Drift < -0.05 THEN 'Buy'
        ELSE 'Hold'
    END AS ActionSignal,
    -- Amount to Trade ~ TotalValue * Drift * -1
    (TotalAccountValue * Drift * -1) AS TradeAmountEst
FROM RoboAdvisorDB.Silver.DriftAnalysis
WHERE ABS(Drift) > 0.05;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all 'Sell' signals from RoboAdvisorDB.Gold.RebalanceSignals."
*/
