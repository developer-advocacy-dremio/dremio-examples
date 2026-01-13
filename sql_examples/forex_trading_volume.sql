/*
 * Forex Trading Volume Demo
 * 
 * Scenario:
 * Monitoring currency pair volumes and spread analysis for an FX desk.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Forex;
CREATE FOLDER IF NOT EXISTS RetailDB.Forex.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Forex.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Forex.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Forex.Bronze.Ticks (
    TickID INT,
    Pair VARCHAR,
    Bid DOUBLE,
    Ask DOUBLE,
    Volume INT,
    "Timestamp" TIMESTAMP
);

INSERT INTO RetailDB.Forex.Bronze.Ticks VALUES
(1, 'EUR/USD', 1.0500, 1.0502, 100000, '2025-01-01 10:00:00'),
(2, 'EUR/USD', 1.0501, 1.0503, 150000, '2025-01-01 10:00:01'),
(3, 'USD/JPY', 145.00, 145.05, 50000, '2025-01-01 10:00:00'),
(4, 'GBP/USD', 1.2200, 1.2204, 80000, '2025-01-01 10:00:00'),
(5, 'EUR/USD', 1.0502, 1.0504, 120000, '2025-01-01 10:00:02'),
(6, 'USD/JPY', 145.02, 145.07, 60000, '2025-01-01 10:00:01'),
(7, 'GBP/USD', 1.2198, 1.2202, 75000, '2025-01-01 10:00:01'),
(8, 'USD/CHF', 0.8900, 0.8903, 40000, '2025-01-01 10:00:00'),
(9, 'AUD/USD', 0.6500, 0.6502, 90000, '2025-01-01 10:00:00'),
(10, 'EUR/USD', 1.0505, 1.0507, 200000, '2025-01-01 10:00:03');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Forex.Silver.Spreads AS
SELECT 
    TickID,
    Pair,
    Bid,
    Ask,
    (Ask - Bid) AS Spread,
    Volume,
    "Timestamp"
FROM RetailDB.Forex.Bronze.Ticks;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Forex.Gold.PairLiquidity AS
SELECT 
    Pair,
    AVG(Spread) AS AvgSpread,
    SUM(Volume) AS TotalVolume
FROM RetailDB.Forex.Silver.Spreads
GROUP BY Pair
ORDER BY TotalVolume DESC;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the most liquid currency pair (highest volume) in RetailDB.Forex.Gold.PairLiquidity?"
*/
