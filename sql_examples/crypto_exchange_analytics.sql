/*
 * Crypto Exchange Analytics Demo
 * 
 * Scenario:
 * Analyzing trading volume, gas fees, and cold storage balances.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Crypto;
CREATE FOLDER IF NOT EXISTS RetailDB.Crypto.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Crypto.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Crypto.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Crypto.Bronze.Trades (
    TradeID INT,
    Pair VARCHAR, -- e.g., BTC-USD
    Side VARCHAR, -- Buy/Sell
    Amount DOUBLE,
    Price DOUBLE,
    GasFee DOUBLE,
    Timestamp TIMESTAMP
);

INSERT INTO RetailDB.Crypto.Bronze.Trades VALUES
(1, 'BTC-USD', 'Buy', 0.5, 45000.00, 15.00, '2025-01-01 10:00:00'),
(2, 'BTC-USD', 'Sell', 0.2, 45100.00, 12.00, '2025-01-01 10:05:00'),
(3, 'ETH-USD', 'Buy', 10.0, 3000.00, 50.00, '2025-01-01 10:10:00'), -- High gas
(4, 'ETH-USD', 'Sell', 5.0, 3005.00, 45.00, '2025-01-01 10:15:00'),
(5, 'SOL-USD', 'Buy', 100.0, 120.00, 0.05, '2025-01-01 10:20:00'),
(6, 'SOL-USD', 'Sell', 50.0, 121.00, 0.05, '2025-01-01 10:25:00'),
(7, 'BTC-USD', 'Buy', 1.0, 44900.00, 14.00, '2025-01-01 10:30:00'),
(8, 'BTC-USD', 'Sell', 0.1, 44950.00, 11.00, '2025-01-01 10:35:00'),
(9, 'DOGE-USD', 'Buy', 5000.0, 0.15, 0.01, '2025-01-01 11:00:00'),
(10, 'ETH-USD', 'Buy', 2.0, 3010.00, 40.00, '2025-01-01 11:05:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Crypto.Silver.TradeVolume AS
SELECT 
    Pair,
    Side,
    COUNT(*) AS TradeCount,
    SUM(Amount) AS VolumeBase,
    SUM(Amount * Price) AS VolumeUSD,
    AVG(GasFee) AS AvgGasFee
FROM RetailDB.Crypto.Bronze.Trades
GROUP BY Pair, Side;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Crypto.Gold.MarketSummary AS
SELECT 
    Pair,
    SUM(VolumeUSD) AS TotalVolumeUSD,
    AVG(AvgGasFee) AS AvgNetworkCost
FROM RetailDB.Crypto.Silver.TradeVolume
GROUP BY Pair;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which crypto pair has the highest TotalVolumeUSD in RetailDB.Crypto.Gold.MarketSummary?"
*/
