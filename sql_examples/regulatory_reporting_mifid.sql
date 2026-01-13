/*
 * Regulatory Reporting (MiFID II) Demo
 * 
 * Scenario:
 * European compliance requires "Post-Trade Transparency" - publishing trade details 
 * within minutes of execution to a public APA (Approved Publication Arrangement).
 * 
 * Data Context:
 * - Executions: Internal trade log.
 * - ReferenceData: ISIN details (Liquid/Illiquid classification).
 * 
 * Analytical Goal:
 * Filter trades that eligible for "Deferred Publication" (large in scale) versus 
 * those requiring immediate reporting.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MifidReportDB;
CREATE FOLDER IF NOT EXISTS MifidReportDB.Bronze;
CREATE FOLDER IF NOT EXISTS MifidReportDB.Silver;
CREATE FOLDER IF NOT EXISTS MifidReportDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MifidReportDB.Bronze.Executions (
    ExecID VARCHAR,
    ISIN VARCHAR,
    Qty DOUBLE,
    Price DOUBLE,
    Venue VARCHAR, -- 'XLON', 'XPAR', 'OTC'
    ExecTime TIMESTAMP
);

CREATE TABLE IF NOT EXISTS MifidReportDB.Bronze.AssetClassRef (
    ISIN VARCHAR,
    AssetClass VARCHAR, -- 'Equity', 'Bond'
    LiquidityStatus VARCHAR, -- 'Liquid', 'Illiquid'
    LIS_Threshold DOUBLE -- Large In Scale threshold
);

INSERT INTO MifidReportDB.Bronze.Executions VALUES
('E-01', 'US0378331005', 500.0, 150.0, 'XLON', '2025-01-01 09:00:00'),
('E-02', 'US0378331005', 100000.0, 150.0, 'OTC', '2025-01-01 09:05:00'), -- Large Block
('E-03', 'GB0002374006', 5000.0, 5.0, 'XLON', '2025-01-01 09:10:00'),
('E-04', 'GB0002374006', 2000000.0, 5.0, 'OTC', '2025-01-01 09:15:00'), -- Large Block
('E-05', 'US5949181045', 100.0, 250.0, 'XPAR', '2025-01-01 09:20:00'),
('E-06', 'US0378331005', 200.0, 150.0, 'XLON', '2025-01-01 09:30:00'),
('E-07', 'DE000C100003', 10000.0, 100.0, 'XFRA', '2025-01-01 10:00:00'),
('E-08', 'DE000C100003', 500000.0, 100.0, 'OTC', '2025-01-01 10:05:00'), -- LIS
('E-09', 'FR0000120271', 50.0, 120.0, 'XPAR', '2025-01-01 10:10:00'),
('E-10', 'FR0000120271', 60.0, 120.0, 'XPAR', '2025-01-01 10:15:00');

INSERT INTO MifidReportDB.Bronze.AssetClassRef VALUES
('US0378331005', 'Equity', 'Liquid', 10000.0), -- AAPL
('GB0002374006', 'Equity', 'Liquid', 50000.0), -- Diageo
('US5949181045', 'Equity', 'Liquid', 10000.0), -- MSFT
('DE000C100003', 'Bond', 'Illiquid', 100000.0), -- Siemens Bond
('FR0000120271', 'Equity', 'Liquid', 5000.0); -- TotalEnergies

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MifidReportDB.Silver.ReportingLogic AS
SELECT 
    e.ExecID,
    e.ISIN,
    e.Qty,
    e.Price,
    (e.Qty * e.Price) AS NotionalValue,
    r.AssetClass,
    r.LiquidityStatus,
    r.LIS_Threshold,
    CASE 
        WHEN (e.Qty * e.Price) >= r.LIS_Threshold THEN 'LIS_Deferral_Allowed'
        WHEN r.LiquidityStatus = 'Illiquid' THEN 'Illiquid_Deferral_Allowed'
        ELSE 'Immediate_Publication'
    END AS ReportingObligation
FROM MifidReportDB.Bronze.Executions e
JOIN MifidReportDB.Bronze.AssetClassRef r ON e.ISIN = r.ISIN;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MifidReportDB.Gold.TodaysReport AS
SELECT 
    ReportingObligation,
    COUNT(*) AS TradeCount,
    SUM(NotionalValue) AS TotalVolumeEUR
FROM MifidReportDB.Silver.ReportingLogic
GROUP BY ReportingObligation;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all trades that require 'Immediate_Publication' from MifidReportDB.Silver.ReportingLogic."
*/
