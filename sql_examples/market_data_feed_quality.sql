/*
 * Market Data Feed Quality Monitor Demo
 * 
 * Scenario:
 * A trading firm consumes real-time tick data from multiple vendors (e.g., Bloomberg, Refinitiv).
 * They need to detect "stale" feeds or missing ticks (Gaps) to prevent Algo mispricing.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Flag sources with reliability issues (Gaps > 5 seconds).
 * 
 * Note: Assumes a catalog named 'MarketDataOps' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS MarketDataOps;
CREATE FOLDER IF NOT EXISTS MarketDataOps.Bronze;
CREATE FOLDER IF NOT EXISTS MarketDataOps.Silver;
CREATE FOLDER IF NOT EXISTS MarketDataOps.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Ticks
-------------------------------------------------------------------------------
-- Description: Ingesting high-frequency price updates.

CREATE TABLE IF NOT EXISTS MarketDataOps.Bronze.RawTickFeed (
    TickID BIGINT,
    Symbol VARCHAR,
    Price DOUBLE,
    SourceID VARCHAR, -- 'VendorA', 'VendorB'
    TickTimestamp TIMESTAMP
);

-- 1.1 Populate RawTickFeed (50+ Records)
-- Simulating a stream where VendorB has a gap.
INSERT INTO MarketDataOps.Bronze.RawTickFeed (TickID, Symbol, Price, SourceID, TickTimestamp) VALUES
(1, 'AAPL', 150.01, 'VendorA', TIMESTAMP '2025-01-20 09:30:00'),
(2, 'AAPL', 150.02, 'VendorA', TIMESTAMP '2025-01-20 09:30:01'),
(3, 'AAPL', 150.03, 'VendorB', TIMESTAMP '2025-01-20 09:30:00'),
(4, 'AAPL', 150.04, 'VendorB', TIMESTAMP '2025-01-20 09:30:01'),
(5, 'AAPL', 150.02, 'VendorA', TIMESTAMP '2025-01-20 09:30:02'),
(6, 'AAPL', 150.01, 'VendorA', TIMESTAMP '2025-01-20 09:30:03'),
(7, 'AAPL', 150.05, 'VendorB', TIMESTAMP '2025-01-20 09:30:02'), -- VendorB Gap Starts Here
(8, 'AAPL', 150.02, 'VendorA', TIMESTAMP '2025-01-20 09:30:04'),
(9, 'AAPL', 150.03, 'VendorA', TIMESTAMP '2025-01-20 09:30:05'),
(10, 'AAPL', 150.04, 'VendorA', TIMESTAMP '2025-01-20 09:30:06'),
(11, 'AAPL', 150.05, 'VendorA', TIMESTAMP '2025-01-20 09:30:07'),
(12, 'AAPL', 150.06, 'VendorB', TIMESTAMP '2025-01-20 09:30:10'), -- VendorB Resumes (8s gap)
(13, 'AAPL', 150.07, 'VendorA', TIMESTAMP '2025-01-20 09:30:08'),
(14, 'AAPL', 150.08, 'VendorA', TIMESTAMP '2025-01-20 09:30:09'),
(15, 'AAPL', 150.09, 'VendorA', TIMESTAMP '2025-01-20 09:30:10'),
(16, 'GOOG', 2800.01, 'VendorA', TIMESTAMP '2025-01-20 09:30:00'),
(17, 'GOOG', 2800.05, 'VendorB', TIMESTAMP '2025-01-20 09:30:00'),
(18, 'GOOG', 2800.02, 'VendorA', TIMESTAMP '2025-01-20 09:30:01'),
(19, 'GOOG', 2800.06, 'VendorB', TIMESTAMP '2025-01-20 09:30:01'),
(20, 'GOOG', 2800.03, 'VendorA', TIMESTAMP '2025-01-20 09:30:02'),
(21, 'GOOG', 2800.07, 'VendorB', TIMESTAMP '2025-01-20 09:30:02'),
(22, 'GOOG', 2800.04, 'VendorA', TIMESTAMP '2025-01-20 09:30:03'),
(23, 'GOOG', 2800.08, 'VendorB', TIMESTAMP '2025-01-20 09:30:03'),
(24, 'GOOG', 2800.05, 'VendorA', TIMESTAMP '2025-01-20 09:30:04'),
(25, 'GOOG', 2800.09, 'VendorB', TIMESTAMP '2025-01-20 09:30:04'),
(26, 'MSFT', 300.01, 'VendorA', TIMESTAMP '2025-01-20 09:30:00'),
(27, 'MSFT', 300.02, 'VendorA', TIMESTAMP '2025-01-20 09:30:01'),
(28, 'MSFT', 300.03, 'VendorA', TIMESTAMP '2025-01-20 09:30:02'),
(29, 'MSFT', 300.04, 'VendorA', TIMESTAMP '2025-01-20 09:30:03'),
(30, 'MSFT', 300.05, 'VendorA', TIMESTAMP '2025-01-20 09:30:04'),
(31, 'MSFT', 300.06, 'VendorA', TIMESTAMP '2025-01-20 09:30:05'),
(32, 'MSFT', 300.07, 'VendorA', TIMESTAMP '2025-01-20 09:30:06'),
(33, 'MSFT', 300.08, 'VendorA', TIMESTAMP '2025-01-20 09:30:07'),
(34, 'MSFT', 300.09, 'VendorA', TIMESTAMP '2025-01-20 09:30:08'),
(35, 'MSFT', 300.10, 'VendorA', TIMESTAMP '2025-01-20 09:30:09'),
(36, 'TSLA', 950.01, 'VendorB', TIMESTAMP '2025-01-20 09:30:00'),
(37, 'TSLA', 950.02, 'VendorB', TIMESTAMP '2025-01-20 09:30:01'),
(38, 'TSLA', 950.03, 'VendorB', TIMESTAMP '2025-01-20 09:30:02'),
(39, 'TSLA', 950.04, 'VendorB', TIMESTAMP '2025-01-20 09:30:03'),
(40, 'TSLA', 950.05, 'VendorB', TIMESTAMP '2025-01-20 09:30:04'),
(41, 'TSLA', 950.06, 'VendorB', TIMESTAMP '2025-01-20 09:30:05'),
(42, 'TSLA', 950.07, 'VendorB', TIMESTAMP '2025-01-20 09:30:06'),
(43, 'TSLA', 950.08, 'VendorB', TIMESTAMP '2025-01-20 09:30:07'),
(44, 'TSLA', 950.09, 'VendorB', TIMESTAMP '2025-01-20 09:30:08'),
(45, 'TSLA', 950.10, 'VendorB', TIMESTAMP '2025-01-20 09:30:09'),
(46, 'AMZN', 3300.01, 'VendorA', TIMESTAMP '2025-01-20 09:30:00'),
(47, 'AMZN', 3300.02, 'VendorA', TIMESTAMP '2025-01-20 09:30:01'),
(48, 'AMZN', 3300.03, 'VendorA', TIMESTAMP '2025-01-20 09:30:02'),
(49, 'AMZN', 3300.04, 'VendorA', TIMESTAMP '2025-01-20 09:30:03'),
(50, 'AMZN', 3300.05, 'VendorA', TIMESTAMP '2025-01-20 09:30:04');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Gap Analysis
-------------------------------------------------------------------------------
-- Description: Calculating the time difference between consecutive ticks for the same Symbol + Source.
-- Feature: Highlighting intervals where Delta > threshold.

CREATE OR REPLACE VIEW MarketDataOps.Silver.TickGaps AS
SELECT
    Symbol,
    SourceID,
    TickTimestamp,
    LAG(TickTimestamp) OVER (PARTITION BY Symbol, SourceID ORDER BY TickTimestamp) AS PrevTimestamp,
    TIMESTAMPDIFF(SECOND, LAG(TickTimestamp) OVER (PARTITION BY Symbol, SourceID ORDER BY TickTimestamp), TickTimestamp) AS TimeDelta
FROM MarketDataOps.Bronze.RawTickFeed;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Feed Reliability
-------------------------------------------------------------------------------
-- Description: Aggregating gap statistics by Vendor.
-- Insight: Determining which vendor has the most outages.

CREATE OR REPLACE VIEW MarketDataOps.Gold.FeedReliabilityScore AS
SELECT
    SourceID,
    COUNT(*) AS TotalTicks,
    SUM(CASE WHEN TimeDelta > 5 THEN 1 ELSE 0 END) AS MajorGaps_Gt5s,
    MAX(TimeDelta) AS MaxGapSeconds,
    AVG(TimeDelta) AS AvgLatency
FROM MarketDataOps.Silver.TickGaps
WHERE PrevTimestamp IS NOT NULL
GROUP BY SourceID;

CREATE OR REPLACE VIEW MarketDataOps.Gold.BadDataDetection AS
SELECT * 
FROM MarketDataOps.Silver.TickGaps
WHERE TimeDelta > 5
ORDER BY TimeDelta DESC;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which SourceID has the worst reliability based on MarketDataOps.Gold.FeedReliabilityScore?"

PROMPT:
"Show me all major gaps > 5 seconds from MarketDataOps.Gold.BadDataDetection."
*/
