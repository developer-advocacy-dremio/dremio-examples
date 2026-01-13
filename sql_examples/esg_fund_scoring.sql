/*
 * ESG Fund Scoring Demo
 * 
 * Scenario:
 * Evaluating investment portfolios based on Environmental, Social, and Governance (ESG) metrics.
 * 
 * Data Context:
 * - Holdings: Stocks held by each fund.
 * - CompanyScores: Third-party ESG ratings for each ticker.
 * 
 * Analytical Goal:
 * Calculate the weighted average ESG score for each fund to ensure mandate compliance (e.g., "Sustainable Growth Fund").
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ESGFundDB;
CREATE FOLDER IF NOT EXISTS ESGFundDB.Bronze;
CREATE FOLDER IF NOT EXISTS ESGFundDB.Silver;
CREATE FOLDER IF NOT EXISTS ESGFundDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ESGFundDB.Bronze.FundHoldings (
    FundID VARCHAR,
    Ticker VARCHAR,
    Shares INT,
    PriceClose DOUBLE
);

CREATE TABLE IF NOT EXISTS ESGFundDB.Bronze.CompanyScores (
    Ticker VARCHAR,
    CompanyName VARCHAR,
    EnvScore INT, -- 0-100
    SocScore INT,
    GovScore INT
);

INSERT INTO ESGFundDB.Bronze.FundHoldings VALUES
('GreenFund', 'AAPL', 5000, 150.0), -- 750k
('GreenFund', 'TSLA', 1000, 200.0), -- 200k
('GreenFund', 'XOM', 500, 100.0),   -- 50k
('GreenFund', 'MSFT', 3000, 300.0), -- 900k
('GrowthFund', 'AAPL', 2000, 150.0),
('GrowthFund', 'AMZN', 1000, 100.0),
('GrowthFund', 'FB', 500, 180.0),
('ValueFund', 'XOM', 10000, 100.0),
('ValueFund', 'CVX', 5000, 150.0),
('ValueFund', 'JNJ', 2000, 160.0),
('TechFund', 'MSFT', 5000, 300.0),
('TechFund', 'NVDA', 2000, 400.0);

INSERT INTO ESGFundDB.Bronze.CompanyScores VALUES
('AAPL', 'Apple Inc', 80, 85, 90),
('TSLA', 'Tesla Inc', 90, 70, 60),
('XOM', 'Exxon Mobil', 40, 50, 70),
('MSFT', 'Microsoft', 95, 90, 95),
('AMZN', 'Amazon', 60, 50, 60),
('FB', 'Meta', 70, 40, 50),
('CVX', 'Chevron', 45, 55, 75),
('JNJ', 'Johnson & Johnson', 70, 80, 85),
('NVDA', 'Nvidia', 85, 80, 85),
('GOOG', 'Alphabet', 80, 80, 80);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ESGFundDB.Silver.WeightedHoldings AS
SELECT 
    h.FundID,
    h.Ticker,
    spot.CompanyName,
    (h.Shares * h.PriceClose) AS MarketValue,
    spot.EnvScore,
    spot.SocScore,
    spot.GovScore
FROM ESGFundDB.Bronze.FundHoldings h
JOIN ESGFundDB.Bronze.CompanyScores spot ON h.Ticker = spot.Ticker;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ESGFundDB.Gold.FundRatings AS
SELECT 
    FundID,
    SUM(MarketValue) AS TotalAUM,
    SUM(MarketValue * EnvScore) / SUM(MarketValue) AS Wgt_Avg_Env,
    SUM(MarketValue * SocScore) / SUM(MarketValue) AS Wgt_Avg_Soc,
    SUM(MarketValue * GovScore) / SUM(MarketValue) AS Wgt_Avg_Gov,
    (
      (SUM(MarketValue * EnvScore) / SUM(MarketValue)) + 
      (SUM(MarketValue * SocScore) / SUM(MarketValue)) + 
      (SUM(MarketValue * GovScore) / SUM(MarketValue))
    ) / 3.0 AS CompositeESG
FROM ESGFundDB.Silver.WeightedHoldings
GROUP BY FundID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Rank the funds by CompositeESG score in descending order from ESGFundDB.Gold.FundRatings."

PROMPT:
"Which fund has the lowest Wgt_Avg_Env score?"
*/
