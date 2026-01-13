/*
 * Forex Desk P&L Demo
 * 
 * Scenario:
 * A bank's FX dealing desk tracking real-time positions and P&L across currency pairs.
 * 
 * Data Context:
 * - Trades: Buy/Sell orders executed by traders.
 * - LiveRates: Current market mid-rates.
 * 
 * Analytical Goal:
 * Calculate Net Open Position (NOP) and Unrealized P&L in USD.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ForexDeskDB;
CREATE FOLDER IF NOT EXISTS ForexDeskDB.Bronze;
CREATE FOLDER IF NOT EXISTS ForexDeskDB.Silver;
CREATE FOLDER IF NOT EXISTS ForexDeskDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ForexDeskDB.Bronze.Trades (
    TicketID VARCHAR,
    TraderID VARCHAR,
    Pair VARCHAR, -- 'EURUSD', 'GBPUSD'
    Side VARCHAR, -- 'Buy', 'Sell'
    AmountNotional DOUBLE,
    PriceExecuted DOUBLE,
    TradeTime TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ForexDeskDB.Bronze.LiveRates (
    Pair VARCHAR,
    Bid DOUBLE,
    Ask DOUBLE,
    "Timestamp" TIMESTAMP
);

INSERT INTO ForexDeskDB.Bronze.Trades VALUES
('T-01', 'Bob', 'EURUSD', 'Buy', 1000000.0, 1.0500, '2025-11-01 09:00:00'),
('T-02', 'Bob', 'EURUSD', 'Sell', 500000.0, 1.0550, '2025-11-01 09:30:00'),
('T-03', 'Alice', 'GBPUSD', 'Buy', 2000000.0, 1.2000, '2025-11-01 10:00:00'),
('T-04', 'Alice', 'GBPUSD', 'Buy', 1000000.0, 1.2050, '2025-11-01 10:15:00'),
('T-05', 'Charlie', 'USDJPY', 'Buy', 1000000.0, 145.00, '2025-11-01 11:00:00'), -- USD base
('T-06', 'Bob', 'EURUSD', 'Sell', 200000.0, 1.0520, '2025-11-01 12:00:00'),
('T-07', 'Bob', 'EURUSD', 'Buy', 100000.0, 1.0530, '2025-11-01 12:30:00'),
('T-08', 'Alice', 'GBPUSD', 'Sell', 3000000.0, 1.2100, '2025-11-01 13:00:00'), -- Flat
('T-09', 'Charlie', 'USDJPY', 'Sell', 500000.0, 146.00, '2025-11-01 14:00:00'),
('T-10', 'Dave', 'AUDUSD', 'Buy', 500000.0, 0.6500, '2025-11-01 09:00:00');

INSERT INTO ForexDeskDB.Bronze.LiveRates VALUES
('EURUSD', 1.0510, 1.0512, '2025-11-01 15:00:00'),
('GBPUSD', 1.2080, 1.2085, '2025-11-01 15:00:00'),
('USDJPY', 145.50, 145.55, '2025-11-01 15:00:00'),
('AUDUSD', 0.6550, 0.6555, '2025-11-01 15:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ForexDeskDB.Silver.Positions AS
SELECT 
    TraderID,
    Pair,
    SUM(CASE WHEN Side = 'Buy' THEN AmountNotional ELSE -AmountNotional END) AS NetPosition,
    SUM(CASE WHEN Side = 'Buy' THEN AmountNotional * PriceExecuted ELSE -AmountNotional * PriceExecuted END) AS CostBasisUSD
FROM ForexDeskDB.Bronze.Trades
GROUP BY TraderID, Pair;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ForexDeskDB.Gold.DeskPnL AS
SELECT 
    p.TraderID,
    p.Pair,
    p.NetPosition,
    (r.Bid + r.Ask) / 2.0 AS CurrentMidRate,
    -- Simple Val Calculation (Assuming USD Quote currency for EURUSD/GBPUSD)
    -- PnL = (CurrentValue - CostBasis)
    (p.NetPosition * ((r.Bid + r.Ask) / 2.0)) - p.CostBasisUSD AS FloatingPnL_USD
FROM ForexDeskDB.Silver.Positions p
JOIN ForexDeskDB.Bronze.LiveRates r ON p.Pair = r.Pair;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the total FloatingPnL_USD for TraderID 'Bob' across all pairs in ForexDeskDB.Gold.DeskPnL?"

PROMPT:
"Which Pair has the largest absolute NetPosition currently?"
*/
