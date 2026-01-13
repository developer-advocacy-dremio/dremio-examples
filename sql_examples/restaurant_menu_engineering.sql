/*
 * Restaurant Menu Engineering Demo
 * 
 * Scenario:
 * Categorizing menu items based on profitability and popularity (Stars, Plowhorses, Puzzles, Dogs).
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize menu pricing and layout.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RestaurantDB;
CREATE FOLDER IF NOT EXISTS RestaurantDB.Bronze;
CREATE FOLDER IF NOT EXISTS RestaurantDB.Silver;
CREATE FOLDER IF NOT EXISTS RestaurantDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RestaurantDB.Bronze.MenuItems (
    ItemID INT,
    Name VARCHAR,
    Cost DOUBLE,
    Price DOUBLE,
    Category VARCHAR -- 'Main', 'Appetizer', 'Dessert'
);

CREATE TABLE IF NOT EXISTS RestaurantDB.Bronze.DailySales (
    SalesID INT,
    ItemID INT,
    Date DATE,
    QuantitySold INT
);

INSERT INTO RestaurantDB.Bronze.MenuItems VALUES
(1, 'Signature Burger', 5.00, 15.00, 'Main'),
(2, 'Lobster Bisque', 8.00, 12.00, 'Appetizer'),
(3, 'Fries', 0.50, 5.00, 'Side');

INSERT INTO RestaurantDB.Bronze.DailySales VALUES
(1, 1, '2025-05-01', 100), -- High vol
(2, 2, '2025-05-01', 10), -- Low vol
(3, 3, '2025-05-01', 150); -- High vol

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RestaurantDB.Silver.ItemAnalysis AS
SELECT 
    m.Name,
    m.Category,
    SUM(d.QuantitySold) AS TotalSold,
    (m.Price - m.Cost) AS ContributionMargin,
    SUM(d.QuantitySold) * (m.Price - m.Cost) AS TotalProfit
FROM RestaurantDB.Bronze.MenuItems m
JOIN RestaurantDB.Bronze.DailySales d ON m.ItemID = d.ItemID
GROUP BY m.Name, m.Category, m.Price, m.Cost;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RestaurantDB.Gold.MenuMatrix AS
SELECT 
    Name,
    TotalSold,
    ContributionMargin,
    CASE 
        WHEN TotalSold > 50 AND ContributionMargin > 8 THEN 'Star' -- High Pop, High Profit
        WHEN TotalSold > 50 AND ContributionMargin <= 8 THEN 'Plowhorse' -- High Pop, Low Profit
        WHEN TotalSold <= 50 AND ContributionMargin > 8 THEN 'Puzzle' -- Low Pop, High Profit
        ELSE 'Dog' -- Low Pop, Low Profit
    END AS Classification
FROM RestaurantDB.Silver.ItemAnalysis;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all items classified as 'Star' in RestaurantDB.Gold.MenuMatrix."
*/
