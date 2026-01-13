/*
 * Marketing Multi-Touch Attribution Demo
 * 
 * Scenario:
 * Assigning credit for conversions across multiple touchpoints (Ads, Email, Direct).
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Allocate marketing budget to the most effective channels.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MarketingDB;
CREATE FOLDER IF NOT EXISTS MarketingDB.Bronze;
CREATE FOLDER IF NOT EXISTS MarketingDB.Silver;
CREATE FOLDER IF NOT EXISTS MarketingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MarketingDB.Bronze.Touchpoints (
    TouchID INT,
    CustomerID INT,
    Channel VARCHAR, -- 'Facebook', 'Email', 'GoogleAds'
    TouchTime TIMESTAMP,
    Cost DOUBLE
);

CREATE TABLE IF NOT EXISTS MarketingDB.Bronze.Conversions (
    ConversionID INT,
    CustomerID INT,
    ConversionTime TIMESTAMP,
    Revenue DOUBLE
);

INSERT INTO MarketingDB.Bronze.Touchpoints VALUES
(1, 101, 'Facebook', '2025-01-01 10:00:00', 1.50),
(2, 101, 'Email', '2025-01-02 09:00:00', 0.05),
(3, 101, 'GoogleAds', '2025-01-03 14:00:00', 2.00);

INSERT INTO MarketingDB.Bronze.Conversions VALUES
(1, 101, '2025-01-03 15:00:00', 100.00);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MarketingDB.Silver.CustomerJourney AS
SELECT 
    t.CustomerID,
    t.Channel,
    t.TouchTime,
    c.Revenue,
    ROW_NUMBER() OVER (PARTITION BY t.CustomerID ORDER BY t.TouchTime ASC) AS PathPosition,
    COUNT(*) OVER (PARTITION BY t.CustomerID) AS TotalTouches
FROM MarketingDB.Bronze.Touchpoints t
JOIN MarketingDB.Bronze.Conversions c ON t.CustomerID = c.CustomerID
WHERE t.TouchTime <= c.ConversionTime;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

-- Linear Attribution Model: Equal credit to all touchpoints
CREATE OR REPLACE VIEW MarketingDB.Gold.ChannelAttribution AS
SELECT 
    Channel,
    COUNT(DISTINCT CustomerID) AS AssistedConversions,
    SUM(Revenue / TotalTouches) AS AttributedRevenue
FROM MarketingDB.Silver.CustomerJourney
GROUP BY Channel;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Channel has the highest AttributedRevenue in MarketingDB.Gold.ChannelAttribution?"
*/
