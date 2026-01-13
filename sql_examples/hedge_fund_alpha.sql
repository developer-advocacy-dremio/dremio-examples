/*
 * Hedge Fund Alpha Attribution Demo
 * 
 * Scenario:
 * Decomposing portfolio returns into "Alpha" (Active Return) and "Beta" (Market Return).
 * 
 * Data Context:
 * - DailyReturns: The fund's daily percentage return.
 * - BenchmarkReturns: S&P 500 or aggregate index daily return.
 * 
 * Analytical Goal:
 * Calculate Cumulative Alpha to justify performance fees.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS AlphaGenDB;
CREATE FOLDER IF NOT EXISTS AlphaGenDB.Bronze;
CREATE FOLDER IF NOT EXISTS AlphaGenDB.Silver;
CREATE FOLDER IF NOT EXISTS AlphaGenDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AlphaGenDB.Bronze.FundPerformance (
    FundCode VARCHAR,
    "Date" DATE,
    DailyReturnPct DOUBLE
);

CREATE TABLE IF NOT EXISTS AlphaGenDB.Bronze.BenchmarkDetails (
    IndexCode VARCHAR, -- 'SPX', 'NDX'
    "Date" DATE,
    DailyReturnPct DOUBLE
);

-- 12 Days of Returns
INSERT INTO AlphaGenDB.Bronze.FundPerformance VALUES
('HF-Opportunities', '2025-01-01', 0.015), -- +1.5%
('HF-Opportunities', '2025-01-02', -0.005),-- -0.5%
('HF-Opportunities', '2025-01-03', 0.020),
('HF-Opportunities', '2025-01-04', 0.005),
('HF-Opportunities', '2025-01-05', 0.010),
('HF-Opportunities', '2025-01-06', -0.010),
('HF-Opportunities', '2025-01-07', 0.030), -- Big Win
('HF-Opportunities', '2025-01-08', 0.000),
('HF-Opportunities', '2025-01-09', 0.005),
('HF-Opportunities', '2025-01-10', 0.012),
('HF-Opportunities', '2025-01-11', -0.002),
('HF-Opportunities', '2025-01-12', 0.008);

INSERT INTO AlphaGenDB.Bronze.BenchmarkDetails VALUES
('SPX', '2025-01-01', 0.010), -- +1.0%
('SPX', '2025-01-02', -0.010),-- -1.0% (Fund beat market)
('SPX', '2025-01-03', 0.015),
('SPX', '2025-01-04', 0.005),
('SPX', '2025-01-05', 0.008),
('SPX', '2025-01-06', -0.012),
('SPX', '2025-01-07', 0.010), -- Fund beat market by 2%
('SPX', '2025-01-08', 0.002),
('SPX', '2025-01-09', 0.003),
('SPX', '2025-01-10', 0.010),
('SPX', '2025-01-11', -0.005),
('SPX', '2025-01-12', 0.005);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AlphaGenDB.Silver.Attribution AS
SELECT 
    f."Date",
    f.FundCode,
    f.DailyReturnPct AS FundRet,
    b.DailyReturnPct AS BenchmarkRet,
    (f.DailyReturnPct - b.DailyReturnPct) AS DailyAlpha
FROM AlphaGenDB.Bronze.FundPerformance f
JOIN AlphaGenDB.Bronze.BenchmarkDetails b ON f."Date" = b."Date"
WHERE b.IndexCode = 'SPX';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AlphaGenDB.Gold.CumulativeStats AS
SELECT 
    FundCode,
    SUM(FundRet) AS TotalReturn,
    SUM(BenchmarkRet) AS BenchmarkReturn,
    SUM(DailyAlpha) AS TotalAlpha,
    COUNT(CASE WHEN DailyAlpha > 0 THEN 1 END) AS DaysBeatMarket,
    COUNT(*) AS TotalDays
FROM AlphaGenDB.Silver.Attribution
GROUP BY FundCode;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Visualize DailyAlpha over Time (Date) as a line chart from AlphaGenDB.Silver.Attribution."

PROMPT:
"Calculate the 'Win Rate' (DaysBeatMarket / TotalDays) for the fund in AlphaGenDB.Gold.CumulativeStats."
*/
