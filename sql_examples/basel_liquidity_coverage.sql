/*
 * Basel III Liquidity Coverage Demo
 * 
 * Scenario:
 * Calculating the Liquidity Coverage Ratio (LCR) to ensure the bank has enough 
 * High-Quality Liquid Assets (HQLA) to survive a 30-day stress scenario.
 * 
 * Data Context:
 * - Assets: Cash, Government Bonds, Corporate Debt (HQLA Levels 1, 2A, 2B).
 * - CashOutflows: Expected run-off from deposits and commitments over 30 days.
 * 
 * Analytical Goal:
 * Ensure LCR (Stock of HQLA / Total Net Cash Outflows) >= 100%.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS BaselRiskDB;
CREATE FOLDER IF NOT EXISTS BaselRiskDB.Bronze;
CREATE FOLDER IF NOT EXISTS BaselRiskDB.Silver;
CREATE FOLDER IF NOT EXISTS BaselRiskDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS BaselRiskDB.Bronze.LiquidAssets (
    AssetID VARCHAR,
    AssetType VARCHAR, -- 'Cash', 'SovereignBond', 'CorpBond_AA'
    MarketValue DOUBLE,
    HaircutPct DOUBLE -- 0% for Cash, 15% for Level 2A, etc.
);

CREATE TABLE IF NOT EXISTS BaselRiskDB.Bronze.CashOutflows (
    LiabID VARCHAR,
    LiabilityType VARCHAR, -- 'RetailDeposit_Stable', 'RetailDeposit_Unstable', 'UnsecuredWholesale'
    Amount DOUBLE,
    RunOffFactor DOUBLE -- 5% for Stable, 10%+ for Unstable
);

-- 10+ Asset Records
INSERT INTO BaselRiskDB.Bronze.LiquidAssets VALUES
('A-01', 'Cash', 5000000.0, 0.0),
('A-02', 'Cash', 2000000.0, 0.0),
('A-03', 'SovereignBond', 10000000.0, 0.0),
('A-04', 'SovereignBond', 5000000.0, 0.0),
('A-05', 'CorpBond_AA', 3000000.0, 0.15),
('A-06', 'CorpBond_AA', 1500000.0, 0.15),
('A-07', 'CorpBond_BBB', 500000.0, 0.50), -- High haircut
('A-08', 'Cash', 1200000.0, 0.0),
('A-09', 'SovereignBond', 4000000.0, 0.0),
('A-10', 'CorpBond_AA', 800000.0, 0.15);

-- 10+ Liability Records
INSERT INTO BaselRiskDB.Bronze.CashOutflows VALUES
('L-01', 'RetailDeposit_Stable', 10000000.0, 0.05),
('L-02', 'RetailDeposit_Unstable', 5000000.0, 0.10),
('L-03', 'UnsecuredWholesale', 20000000.0, 1.00), -- 100% runoff
('L-04', 'RetailDeposit_Stable', 3000000.0, 0.05),
('L-05', 'RetailDeposit_Unstable', 4000000.0, 0.10),
('L-06', 'SecuredFunding', 10000000.0, 0.25),
('L-07', 'CommittedFacility', 5000000.0, 0.40),
('L-08', 'RetailDeposit_Stable', 8000000.0, 0.05),
('L-09', 'RetailDeposit_Unstable', 200000.0, 0.10),
('L-10', 'UnsecuredWholesale', 100000.0, 1.00);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BaselRiskDB.Silver.HQL_Calculation AS
SELECT 
    AssetType,
    SUM(MarketValue) AS GrossValue,
    AVG(HaircutPct) AS AvgHaircut,
    SUM(MarketValue * (1 - HaircutPct)) AS HQLA_Value
FROM BaselRiskDB.Bronze.LiquidAssets
GROUP BY AssetType;

CREATE OR REPLACE VIEW BaselRiskDB.Silver.NetOutflow_Calculation AS
SELECT 
    LiabilityType,
    SUM(Amount) AS TotalExposure,
    AVG(RunOffFactor) AS RunOffRate,
    SUM(Amount * RunOffFactor) AS NetCashOutflow
FROM BaselRiskDB.Bronze.CashOutflows
GROUP BY LiabilityType;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BaselRiskDB.Gold.LCR_Report AS
SELECT 
    (SELECT SUM(HQLA_Value) FROM BaselRiskDB.Silver.HQL_Calculation) AS Total_HQLA,
    (SELECT SUM(NetCashOutflow) FROM BaselRiskDB.Silver.NetOutflow_Calculation) AS Total_Net_Outflows,
    (
        (SELECT SUM(HQLA_Value) FROM BaselRiskDB.Silver.HQL_Calculation) / 
        (SELECT SUM(NetCashOutflow) FROM BaselRiskDB.Silver.NetOutflow_Calculation)
    ) * 100 AS LCR_Percentage,
    CASE 
        WHEN (
            (SELECT SUM(HQLA_Value) FROM BaselRiskDB.Silver.HQL_Calculation) / 
            (SELECT SUM(NetCashOutflow) FROM BaselRiskDB.Silver.NetOutflow_Calculation)
        ) >= 1.0 THEN 'Compliant'
        ELSE 'Shortfall'
    END AS ComplianceStatus;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Is our current LCR_Percentage compliant with Basel III requirements in BaselRiskDB.Gold.LCR_Report?"

PROMPT:
"Visualize the breakdown of Total_HQLA by AssetType from BaselRiskDB.Silver.HQL_Calculation."
*/
