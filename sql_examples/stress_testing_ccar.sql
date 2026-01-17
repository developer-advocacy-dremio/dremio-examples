/*
 * Stress Testing (CCAR/DFAST) Simulation Demo
 * 
 * Scenario:
 * A bank needs to project capital losses under various adverse economic scenarios (e.g., Recession, Market Crash).
 * This is a requirement for regulatory compliance (CCAR/DFAST).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Calculate projected portfolio value and losses for each stress scenario.
 * 
 * Note: Assumes a catalog named 'RiskEngineDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS RiskEngineDB;
CREATE FOLDER IF NOT EXISTS RiskEngineDB.Bronze;
CREATE FOLDER IF NOT EXISTS RiskEngineDB.Silver;
CREATE FOLDER IF NOT EXISTS RiskEngineDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Positions & Scenarios
-------------------------------------------------------------------------------
-- Description: Ingesting current portfolio snapshots and macro-economic shock factors defined by the risk committee.

CREATE TABLE IF NOT EXISTS RiskEngineDB.Bronze.PortfolioPositions (
    PositionID VARCHAR,
    AssetClass VARCHAR, -- 'CorporateBond', 'MBS', 'Equity', 'Loan'
    NotionalAmount DOUBLE,
    CurrentValue DOUBLE,
    RiskWeight FLOAT,
    Sector VARCHAR
);

CREATE TABLE IF NOT EXISTS RiskEngineDB.Bronze.StressScenarios (
    ScenarioID VARCHAR, -- 'Baseline', 'Adverse', 'SeverelyAdverse'
    AssetClass VARCHAR,
    ShockFactor FLOAT -- Percentage drop in value (e.g., -0.15 for 15% drop)
);

-- 1.1 Populate PortfolioPositions (50+ Records)
INSERT INTO RiskEngineDB.Bronze.PortfolioPositions (PositionID, AssetClass, NotionalAmount, CurrentValue, RiskWeight, Sector) VALUES
('P-001', 'CorporateBond', 1000000, 980000, 0.5, 'Energy'),
('P-002', 'Equity', 500000, 550000, 1.0, 'Tech'),
('P-003', 'MBS', 2000000, 1950000, 0.2, 'RealEstate'),
('P-004', 'Loan', 750000, 750000, 0.8, 'Consumer'),
('P-005', 'CorporateBond', 1200000, 1180000, 0.5, 'Finance'),
('P-006', 'Equity', 600000, 620000, 1.0, 'Healthcare'),
('P-007', 'MBS', 2500000, 2480000, 0.2, 'RealEstate'),
('P-008', 'Loan', 800000, 800000, 0.8, 'Industrial'),
('P-009', 'CorporateBond', 900000, 890000, 0.5, 'Utilities'),
('P-010', 'Equity', 400000, 410000, 1.0, 'Consumer'),
('P-011', 'MBS', 1500000, 1490000, 0.2, 'RealEstate'),
('P-012', 'Loan', 300000, 300000, 0.8, 'SmallBiz'),
('P-013', 'CorporateBond', 1100000, 1090000, 0.5, 'Energy'),
('P-014', 'Equity', 700000, 750000, 1.0, 'Tech'),
('P-015', 'MBS', 1800000, 1780000, 0.2, 'RealEstate'),
('P-016', 'Loan', 600000, 600000, 0.8, 'Consumer'),
('P-017', 'CorporateBond', 1300000, 1280000, 0.5, 'Finance'),
('P-018', 'Equity', 550000, 560000, 1.0, 'Healthcare'),
('P-019', 'MBS', 2200000, 2180000, 0.2, 'RealEstate'),
('P-020', 'Loan', 900000, 900000, 0.8, 'Industrial'),
('P-021', 'CorporateBond', 950000, 940000, 0.5, 'Utilities'),
('P-022', 'Equity', 450000, 460000, 1.0, 'Consumer'),
('P-023', 'MBS', 1600000, 1590000, 0.2, 'RealEstate'),
('P-024', 'Loan', 350000, 350000, 0.8, 'SmallBiz'),
('P-025', 'CorporateBond', 1050000, 1040000, 0.5, 'Energy'),
('P-026', 'Equity', 650000, 680000, 1.0, 'Tech'),
('P-027', 'MBS', 1900000, 1880000, 0.2, 'RealEstate'),
('P-028', 'Loan', 650000, 650000, 0.8, 'Consumer'),
('P-029', 'CorporateBond', 1250000, 1230000, 0.5, 'Finance'),
('P-030', 'Equity', 580000, 590000, 1.0, 'Healthcare'),
('P-031', 'MBS', 2100000, 2080000, 0.2, 'RealEstate'),
('P-032', 'Loan', 850000, 850000, 0.8, 'Industrial'),
('P-033', 'CorporateBond', 920000, 910000, 0.5, 'Utilities'),
('P-034', 'Equity', 420000, 430000, 1.0, 'Consumer'),
('P-035', 'MBS', 1550000, 1540000, 0.2, 'RealEstate'),
('P-036', 'Loan', 320000, 320000, 0.8, 'SmallBiz'),
('P-037', 'CorporateBond', 1020000, 1010000, 0.5, 'Energy'),
('P-038', 'Equity', 620000, 650000, 1.0, 'Tech'),
('P-039', 'MBS', 1850000, 1830000, 0.2, 'RealEstate'),
('P-040', 'Loan', 620000, 620000, 0.8, 'Consumer'),
('P-041', 'CorporateBond', 1220000, 1200000, 0.5, 'Finance'),
('P-042', 'Equity', 560000, 570000, 1.0, 'Healthcare'),
('P-043', 'MBS', 2150000, 2130000, 0.2, 'RealEstate'),
('P-044', 'Loan', 880000, 880000, 0.8, 'Industrial'),
('P-045', 'CorporateBond', 940000, 930000, 0.5, 'Utilities'),
('P-046', 'Equity', 440000, 450000, 1.0, 'Consumer'),
('P-047', 'MBS', 1580000, 1570000, 0.2, 'RealEstate'),
('P-048', 'Loan', 340000, 340000, 0.8, 'SmallBiz'),
('P-049', 'CorporateBond', 1040000, 1030000, 0.5, 'Energy'),
('P-050', 'Equity', 640000, 670000, 1.0, 'Tech');

-- 1.2 Populate StressScenarios
INSERT INTO RiskEngineDB.Bronze.StressScenarios (ScenarioID, AssetClass, ShockFactor) VALUES
('Baseline', 'CorporateBond', -0.01),
('Baseline', 'Equity', 0.05),
('Baseline', 'MBS', -0.005),
('Baseline', 'Loan', -0.02),
('Adverse', 'CorporateBond', -0.05),
('Adverse', 'Equity', -0.15),
('Adverse', 'MBS', -0.08),
('Adverse', 'Loan', -0.06),
('SeverelyAdverse', 'CorporateBond', -0.12),
('SeverelyAdverse', 'Equity', -0.40),
('SeverelyAdverse', 'MBS', -0.20),
('SeverelyAdverse', 'Loan', -0.10);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Scenario Application
-------------------------------------------------------------------------------
-- Description: Crossing every position with every scenario to calculate the theoretical stressed value.
-- Transformation: ProjectedValue = CurrentValue * (1 + ShockFactor).

CREATE OR REPLACE VIEW RiskEngineDB.Silver.ScenarizedPortfolio AS
SELECT
    p.PositionID,
    p.Sector,
    p.AssetClass,
    p.CurrentValue,
    s.ScenarioID,
    s.ShockFactor,
    (p.CurrentValue * (1 + s.ShockFactor)) AS ProjectedValue,
    (p.CurrentValue - (p.CurrentValue * (1 + s.ShockFactor))) AS ProjectedLoss
FROM RiskEngineDB.Bronze.PortfolioPositions p
CROSS JOIN RiskEngineDB.Bronze.StressScenarios s
WHERE p.AssetClass = s.AssetClass;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Capital Impact
-------------------------------------------------------------------------------
-- Description: Aggregating losses by Scenario and Asset Class to determine capital requirements.
-- Insight: Determining the "Severely Adverse" impact to ensure capital buffers are sufficient.

CREATE OR REPLACE VIEW RiskEngineDB.Gold.CapitalImpactReport AS
SELECT
    ScenarioID,
    AssetClass,
    SUM(CurrentValue) AS TotalExposure,
    SUM(ProjectedLoss) AS TotalProjectedLoss,
    (SUM(ProjectedLoss) / SUM(CurrentValue)) * 100 AS LossPercentage
FROM RiskEngineDB.Silver.ScenarizedPortfolio
GROUP BY ScenarioID, AssetClass;

CREATE OR REPLACE VIEW RiskEngineDB.Gold.WorstCaseScenario AS
SELECT
    ScenarioID,
    SUM(ProjectedLoss) AS Requirements
FROM RiskEngineDB.Gold.CapitalImpactReport
GROUP BY ScenarioID
ORDER BY Requirements DESC;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Using RiskEngineDB.Gold.CapitalImpactReport, show me the TotalProjectedLoss for the 'SeverelyAdverse' scenario breakdown by AssetClass."

PROMPT:
"What is the total capital requirement for the worst-case scenario in RiskEngineDB.Gold.WorstCaseScenario?"
*/
