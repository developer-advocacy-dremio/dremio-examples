/*
 * Gaming Player Economy Demo
 * 
 * Scenario:
 * A game studio tracks in-game currency flow and item purchases.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Balance the game economy and detect fraud.
 * 
 * Note: Assumes a catalog named 'GamingDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS GamingDB;
CREATE FOLDER IF NOT EXISTS GamingDB.Bronze;
CREATE FOLDER IF NOT EXISTS GamingDB.Silver;
CREATE FOLDER IF NOT EXISTS GamingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Economy Logs
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS GamingDB.Bronze.Players (
    PlayerID INT,
    Username VARCHAR,
    Region VARCHAR,
    JoinDate DATE
);

CREATE TABLE IF NOT EXISTS GamingDB.Bronze.Items (
    ItemID INT,
    ItemName VARCHAR,
    Category VARCHAR, -- 'Weapon', 'Skin', 'Consumable'
    PriceGold INT
);

CREATE TABLE IF NOT EXISTS GamingDB.Bronze.Transactions (
    TxID INT,
    PlayerID INT,
    ItemID INT,
    TxDate TIMESTAMP,
    AmountGold INT,
    Context VARCHAR -- 'Shop', 'Trade', 'Loot'
);

-- 1.2 Populate Bronze Tables
INSERT INTO GamingDB.Bronze.Players (PlayerID, Username, Region, JoinDate) VALUES
(1, 'NoobMaster69', 'NA', '2024-01-01'),
(2, 'ProGamerXYZ', 'EU', '2023-05-15'),
(3, 'CasualDave', 'NA', '2025-02-20');

INSERT INTO GamingDB.Bronze.Items (ItemID, ItemName, Category, PriceGold) VALUES
(101, 'Excalibur', 'Weapon', 5000),
(102, 'Health Potion', 'Consumable', 50),
(103, 'Dragon Skin', 'Skin', 1200);

INSERT INTO GamingDB.Bronze.Transactions (TxID, PlayerID, ItemID, TxDate, AmountGold, Context) VALUES
(1, 1, 102, '2025-07-01 10:00:00', 50, 'Shop'),
(2, 2, 101, '2025-07-01 12:30:00', 5000, 'Shop'),
(3, 1, 103, '2025-07-02 09:15:00', 1200, 'Shop'),
(4, 3, 102, '2025-07-02 11:00:00', 50, 'Loot');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Spending Habits
-------------------------------------------------------------------------------

-- 2.1 View: Player_Spend
-- Aggregates spending by player and category.
CREATE OR REPLACE VIEW GamingDB.Silver.Player_Spend AS
SELECT
    t.PlayerID,
    p.Username,
    i.Category,
    i.ItemName,
    t.AmountGold,
    t.TxDate
FROM GamingDB.Bronze.Transactions t
JOIN GamingDB.Bronze.Players p ON t.PlayerID = p.PlayerID
JOIN GamingDB.Bronze.Items i ON t.ItemID = i.ItemID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Economy Health
-------------------------------------------------------------------------------

-- 3.1 View: Top_Selling_Items
-- Ranks items by revenue.
CREATE OR REPLACE VIEW GamingDB.Gold.Top_Selling_Items AS
SELECT
    ItemName,
    Category,
    COUNT(TxDate) AS UnitsSold,
    SUM(AmountGold) AS TotalRevenue
FROM GamingDB.Silver.Player_Spend
GROUP BY ItemName, Category;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Best Sellers):
"Which item generated the most TotalRevenue in GamingDB.Gold.Top_Selling_Items?"

PROMPT 2 (Whales):
"Find players in GamingDB.Silver.Player_Spend who spent more than 4000 gold."
*/
