/*
 * Credit Card Fraud Detection Demo
 * 
 * Scenario:
 * Detecting potential fraud by monitoring transaction velocity and high-value anomalies.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.FraudDetection;
CREATE FOLDER IF NOT EXISTS RetailDB.FraudDetection.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.FraudDetection.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.FraudDetection.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.FraudDetection.Bronze.CardTransactions (
    TxID VARCHAR,
    CardID VARCHAR,
    Merchant VARCHAR,
    Amount DOUBLE,
    Location VARCHAR,
    TxTime TIMESTAMP
);

INSERT INTO RetailDB.FraudDetection.Bronze.CardTransactions VALUES
('TX001', 'CARD_123', 'Amazon', 45.50, 'US-NY', '2025-02-01 10:00:00'),
('TX002', 'CARD_123', 'Starbucks', 5.25, 'US-NY', '2025-02-01 10:05:00'),
('TX003', 'CARD_123', 'Best Buy', 1200.00, 'US-CA', '2025-02-01 10:30:00'), -- Suspicious: Geo jump
('TX004', 'CARD_456', 'Target', 85.00, 'US-TX', '2025-02-01 11:00:00'),
('TX005', 'CARD_456', 'Walmart', 30.00, 'US-TX', '2025-02-01 11:15:00'),
('TX006', 'CARD_789', 'LuxuryWatch', 5000.00, 'UK-LON', '2025-02-01 12:00:00'),
('TX007', 'CARD_789', 'LuxuryWatch', 5000.00, 'UK-LON', '2025-02-01 12:01:00'), -- Suspicious: Duplicate high value
('TX008', 'CARD_999', 'Gas Station', 40.00, 'US-FL', '2025-02-01 09:00:00'),
('TX009', 'CARD_999', 'Gas Station', 40.00, 'US-FL', '2025-02-01 09:10:00'),
('TX010', 'CARD_999', 'Gas Station', 40.00, 'US-FL', '2025-02-01 09:20:00'), -- Suspicious: Velocity
('TX011', 'CARD_111', 'Uber', 15.00, 'US-CHI', '2025-02-01 14:00:00'),
('TX012', 'CARD_222', 'Netflix', 12.00, 'US-NY', '2025-02-01 15:00:00'),
('TX013', 'CARD_333', 'Apple Store', 999.00, 'US-SF', '2025-02-01 16:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.FraudDetection.Silver.FlaggedTransactions AS
SELECT 
    TxID,
    CardID,
    Merchant,
    Amount,
    Location,
    TxTime,
    CASE 
        WHEN Amount > 2000 THEN 'High Value'
        ELSE 'Normal'
    END AS ValueCategory
FROM RetailDB.FraudDetection.Bronze.CardTransactions;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.FraudDetection.Gold.PotentialFraud AS
SELECT 
    CardID,
    COUNT(*) AS TxCount,
    SUM(Amount) AS TotalAmount,
    MIN(Location) AS FirstLoc,
    MAX(Location) AS LastLoc
FROM RetailDB.FraudDetection.Silver.FlaggedTransactions
GROUP BY CardID
HAVING COUNT(*) >= 3 OR SUM(Amount) > 3000 OR MIN(Location) <> MAX(Location);

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT 1:
"Identify all cards in RetailDB.FraudDetection.Gold.PotentialFraud that have transactions in conflicting locations."
*/
