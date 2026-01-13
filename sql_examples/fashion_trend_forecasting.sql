/*
 * Fashion Trend Forecasting Demo
 * 
 * Scenario:
 * Analyzing social media mentions, search trends, and sales data to predict next season's hits.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Reduce inventory risk and align production with demand.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS FashionDB;
CREATE FOLDER IF NOT EXISTS FashionDB.Bronze;
CREATE FOLDER IF NOT EXISTS FashionDB.Silver;
CREATE FOLDER IF NOT EXISTS FashionDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FashionDB.Bronze.SocialSignals (
    SignalID INT,
    Platform VARCHAR, -- 'Instagram', 'TikTok'
    TrendsTag VARCHAR, -- e.g., 'BaggyJeans', 'NeonColors'
    MentionsCount INT,
    "Date" DATE
);

CREATE TABLE IF NOT EXISTS FashionDB.Bronze.SalesHistory (
    SalesID INT,
    ProductCategory VARCHAR, -- 'Jeans', 'Tops'
    StyleAttribute VARCHAR, -- 'Baggy', 'Slim'
    UnitsSold INT,
    "Date" DATE
);

INSERT INTO FashionDB.Bronze.SocialSignals VALUES
(1, 'TikTok', 'BaggyJeans', 5000, '2025-03-01'),
(2, 'Instagram', 'NeonColors', 1200, '2025-03-01'),
(3, 'TikTok', 'BaggyJeans', 8000, '2025-03-08'); -- Trend rising

INSERT INTO FashionDB.Bronze.SalesHistory VALUES
(101, 'Jeans', 'Baggy', 200, '2025-03-01'),
(102, 'Jeans', 'Slim', 1500, '2025-03-01'), -- Legacy trend
(103, 'Jeans', 'Baggy', 400, '2025-03-08'); -- Sales rising

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FashionDB.Silver.TrendCorrelations AS
SELECT 
    s."Date",
    ss.TrendsTag,
    ss.MentionsCount,
    sh.UnitsSold
FROM FashionDB.Bronze.SocialSignals ss
JOIN FashionDB.Bronze.SalesHistory sh 
  ON ss."Date" = sh."Date" 
  AND ss.TrendsTag = CONCAT(sh.ProductCategory, sh.StyleAttribute); -- Simple correlation key

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FashionDB.Gold.EmergingTrends AS
SELECT 
    TrendsTag,
    SUM(MentionsCount) AS TotalMentions,
    SUM(UnitsSold) AS TotalSales,
    -- Simple slope check could go here
    'High Growth' AS TrendStatus
FROM FashionDB.Silver.TrendCorrelations
GROUP BY TrendsTag
HAVING SUM(MentionsCount) > 10000;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all 'High Growth' trends from FashionDB.Gold.EmergingTrends."
*/
