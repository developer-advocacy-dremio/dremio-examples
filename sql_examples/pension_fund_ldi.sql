/*
 * Pension Fund LDI (Liability Driven Investment) Demo
 * 
 * Scenario:
 * Matching the duration of assets (Bonds) to liabilities (Future Benefit Payments).
 * If interest rates fall, liability values rise; assets must hedge this risk.
 * 
 * Data Context:
 * - Assets: Long-dated bonds.
 * - Liabilities: Projected cash outflows for retirees.
 * 
 * Analytical Goal:
 * Calculate Duration Gap (Asset Duration - Liability Duration) to hedge rate risk.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS PensionLDI_DB;
CREATE FOLDER IF NOT EXISTS PensionLDI_DB.Bronze;
CREATE FOLDER IF NOT EXISTS PensionLDI_DB.Silver;
CREATE FOLDER IF NOT EXISTS PensionLDI_DB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PensionLDI_DB.Bronze.AssetHoldings (
    AssetID VARCHAR,
    Type VARCHAR, -- 'GovBond_30Y', 'CorpBond_10Y'
    MarketValue DOUBLE,
    DurationYears DOUBLE -- Sensitivity to 1% rate change
);

CREATE TABLE IF NOT EXISTS PensionLDI_DB.Bronze.LiabilityStream (
    StreamID VARCHAR,
    PaymentDate DATE,
    Amount DOUBLE,
    DiscountRate DOUBLE -- Used to calculate PV
);

INSERT INTO PensionLDI_DB.Bronze.AssetHoldings VALUES
('A-01', 'GovBond_30Y', 50000000.0, 20.0),
('A-02', 'GovBond_30Y', 30000000.0, 19.5),
('A-03', 'CorpBond_10Y', 20000000.0, 8.0),
('A-04', 'CorpBond_10Y', 10000000.0, 7.5),
('A-05', 'Cash', 5000000.0, 0.0),
('A-06', 'Equities', 40000000.0, 0.0), -- Zero duration assumption for simplicity
('A-07', 'GovBond_20Y', 25000000.0, 15.0),
('A-08', 'GovBond_10Y', 15000000.0, 9.0),
('A-09', 'CorpBond_5Y', 10000000.0, 4.0),
('A-10', 'Derivatives', 5000000.0, 0.0);

INSERT INTO PensionLDI_DB.Bronze.LiabilityStream VALUES
('L-01', '2030-01-01', 1000000.0, 0.04),
('L-02', '2035-01-01', 2000000.0, 0.04),
('L-03', '2040-01-01', 5000000.0, 0.04),
('L-04', '2045-01-01', 8000000.0, 0.04),
('L-05', '2050-01-01', 10000000.0, 0.04),
('L-06', '2055-01-01', 12000000.0, 0.04), -- Long tail
('L-07', '2060-01-01', 15000000.0, 0.04),
('L-08', '2032-01-01', 1500000.0, 0.04),
('L-09', '2037-01-01', 2500000.0, 0.04),
('L-10', '2042-01-01', 6000000.0, 0.04);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PensionLDI_DB.Silver.WeightedDuration AS
SELECT 
    'Assets' AS Category,
    SUM(MarketValue * DurationYears) / SUM(MarketValue) AS PortfolioDuration,
    SUM(MarketValue) AS TotalPV
FROM PensionLDI_DB.Bronze.AssetHoldings
UNION ALL
SELECT 
    'Liabilities' AS Category,
    15.0 AS PortfolioDuration, -- Simplified assumption for liability duration
    SUM(Amount / POWER(1 + DiscountRate, 25)) AS TotalPV -- Rough PV
FROM PensionLDI_DB.Bronze.LiabilityStream;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PensionLDI_DB.Gold.FundingStatus AS
SELECT 
    MAX(CASE WHEN Category = 'Assets' THEN TotalPV END) AS AssetPV,
    MAX(CASE WHEN Category = 'Liabilities' THEN TotalPV END) AS LiabilityPV,
    (MAX(CASE WHEN Category = 'Assets' THEN TotalPV END) / MAX(CASE WHEN Category = 'Liabilities' THEN TotalPV END)) * 100 AS FundingRatioPct,
    MAX(CASE WHEN Category = 'Assets' THEN PortfolioDuration END) - MAX(CASE WHEN Category = 'Liabilities' THEN PortfolioDuration END) AS DurationGap
FROM PensionLDI_DB.Silver.WeightedDuration;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Is there a DurationGap in PensionLDI_DB.Gold.FundingStatus? A negative gap means we are exposed to falling rates."
*/
