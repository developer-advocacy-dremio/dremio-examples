/*
 * Merchant Chargeback Analysis Demo
 * 
 * Scenario:
 * A Payment Processor auditing merchants with high chargeback rates (ratio of Disputes to Sales).
 * High chargebacks (>1%) can lead to heavy fines from Visa/Mastercard.
 * 
 * Data Context:
 * - Sales: Daily transaction volume.
 * - Disputes: Chargebacks received.
 * 
 * Analytical Goal:
 * Calculate the Chargeback Ratio (Count / Count) and Chargeback Volume Ratio (Amount / Amount).
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS PayProcDB;
CREATE FOLDER IF NOT EXISTS PayProcDB.Bronze;
CREATE FOLDER IF NOT EXISTS PayProcDB.Silver;
CREATE FOLDER IF NOT EXISTS PayProcDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PayProcDB.Bronze.DailySales (
    MerchantID VARCHAR,
    "Date" DATE,
    TxnCount INT,
    TotalVolume DOUBLE
);

CREATE TABLE IF NOT EXISTS PayProcDB.Bronze.Disputes (
    DisputeID VARCHAR,
    MerchantID VARCHAR,
    DisputeDate DATE,
    OriginalTxnDate DATE,
    ReasonCode VARCHAR, -- 'Fraud', 'GoodsNotReceived'
    Amount DOUBLE
);

INSERT INTO PayProcDB.Bronze.DailySales VALUES
('M-100', '2025-01-01', 500, 50000.0),
('M-100', '2025-01-02', 450, 45000.0),
('M-100', '2025-01-03', 600, 60000.0),
('M-101', '2025-01-01', 50, 5000.0),
('M-101', '2025-01-02', 40, 4000.0),
('M-102', '2025-01-01', 1000, 10000.0), -- High vol, low ticket
('M-102', '2025-01-02', 1200, 12000.0),
('M-103', '2025-01-01', 100, 20000.0), -- High ticket
('M-103', '2025-01-02', 110, 22000.0),
('M-100', '2025-02-01', 500, 50000.0);

INSERT INTO PayProcDB.Bronze.Disputes VALUES
('D-01', 'M-100', '2025-01-15', '2025-01-01', 'Fraud', 100.0),
('D-02', 'M-100', '2025-01-16', '2025-01-02', 'Fraud', 100.0),
('D-03', 'M-101', '2025-01-15', '2025-01-01', 'GoodsNotReceived', 500.0), -- 1 dispute on 50 txns? 2% potential
('D-04', 'M-101', '2025-01-18', '2025-01-01', 'Fraud', 200.0),
('D-05', 'M-103', '2025-01-20', '2025-01-01', 'ServiceNotDesc', 2000.0),
('D-06', 'M-100', '2025-02-05', '2025-02-01', 'Fraud', 50.0),
('D-07', 'M-100', '2025-02-06', '2025-02-01', 'Fraud', 50.0),
('D-08', 'M-101', '2025-02-01', '2025-01-02', 'Fraud', 100.0),
('D-09', 'M-102', '2025-01-10', '2025-01-01', 'Fraud', 10.0),
('D-10', 'M-102', '2025-01-11', '2025-01-01', 'Fraud', 10.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PayProcDB.Silver.MonthlyAggregates AS
SELECT 
    EXTRACT(MONTH FROM "Date") AS MonthNum,
    MerchantID,
    SUM(TxnCount) AS TotalTxns,
    SUM(TotalVolume) AS SalesVol
FROM PayProcDB.Bronze.DailySales
GROUP BY EXTRACT(MONTH FROM "Date"), MerchantID;

CREATE OR REPLACE VIEW PayProcDB.Silver.MonthlyDisputes AS
SELECT 
    EXTRACT(MONTH FROM OriginalTxnDate) AS MonthNum, -- Match to sales month (Vintage Analysis)
    MerchantID,
    COUNT(*) AS DisputeCount,
    SUM(Amount) AS DisputeVol
FROM PayProcDB.Bronze.Disputes
GROUP BY EXTRACT(MONTH FROM OriginalTxnDate), MerchantID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PayProcDB.Gold.WatchList AS
SELECT 
    s.MerchantID,
    s.MonthNum,
    s.TotalTxns,
    d.DisputeCount,
    (CAST(d.DisputeCount AS DOUBLE) / NULLIF(s.TotalTxns, 0)) * 100 AS DisputeCountRatio,
    CASE 
        WHEN (CAST(d.DisputeCount AS DOUBLE) / NULLIF(s.TotalTxns, 0)) * 100 > 1.0 THEN 'High Risk'
        WHEN (CAST(d.DisputeCount AS DOUBLE) / NULLIF(s.TotalTxns, 0)) * 100 > 0.5 THEN 'Monitoring'
        ELSE 'Good'
    END AS ComplianceStatus
FROM PayProcDB.Silver.MonthlyAggregates s
LEFT JOIN PayProcDB.Silver.MonthlyDisputes d 
    ON s.MerchantID = d.MerchantID AND s.MonthNum = d.MonthNum;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Identify all merchants with 'High Risk' ComplianceStatus in PayProcDB.Gold.WatchList."
*/
