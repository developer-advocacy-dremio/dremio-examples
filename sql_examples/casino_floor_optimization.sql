/*
 * Casino Floor Optimization Demo
 * 
 * Scenario:
 * Analyzing slot machine performance (coin-in vs payout) and table game occupancy.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Maximize revenue per square foot and optimize floor layout.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS CasinoDB;
CREATE FOLDER IF NOT EXISTS CasinoDB.Bronze;
CREATE FOLDER IF NOT EXISTS CasinoDB.Silver;
CREATE FOLDER IF NOT EXISTS CasinoDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CasinoDB.Bronze.SlotMachines (
    AssetID INT,
    GameTitle VARCHAR,
    LocationZone VARCHAR, -- 'HighLimit', 'MainFloor', 'Smoking'
    Denomination DOUBLE,
    Manufacturer VARCHAR
);

CREATE TABLE IF NOT EXISTS CasinoDB.Bronze.SlotSessions (
    SessionID INT,
    AssetID INT,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    CoinIn DOUBLE,
    CoinOut DOUBLE,
    JackpotPaid DOUBLE
);

INSERT INTO CasinoDB.Bronze.SlotMachines VALUES
(1001, 'Lucky 7s', 'MainFloor', 0.25, 'IGT'),
(1002, 'Wheel of Gold', 'MainFloor', 1.00, 'Aristocrat'),
(1003, 'Dragon Fortune', 'HighLimit', 5.00, 'Konami'),
(1004, 'Buffalo Stampede', 'Smoking', 0.50, 'Aristocrat');

INSERT INTO CasinoDB.Bronze.SlotSessions VALUES
(1, 1001, '2025-01-01 20:00:00', '2025-01-01 21:00:00', 500.0, 450.0, 0.0), -- 90% RTP
(2, 1003, '2025-01-01 22:00:00', '2025-01-01 22:30:00', 2000.0, 1500.0, 0.0), -- High roller
(3, 1004, '2025-01-02 14:00:00', '2025-01-02 16:00:00', 800.0, 1200.0, 0.0), -- Player won
(4, 1002, '2025-01-02 18:00:00', '2025-01-02 19:00:00', 300.0, 50.0, 0.0); -- House won big

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CasinoDB.Silver.MachineDailyStats AS
SELECT 
    s.AssetID,
    m.GameTitle,
    m.LocationZone,
    CAST(s.StartTime AS DATE) AS GamingDate,
    SUM(s.CoinIn) AS TotalCoinIn,
    SUM(s.CoinOut + s.JackpotPaid) AS TotalPaid,
    (SUM(s.CoinIn) - SUM(s.CoinOut + s.JackpotPaid)) AS NetWin
FROM CasinoDB.Bronze.SlotSessions s
JOIN CasinoDB.Bronze.SlotMachines m ON s.AssetID = m.AssetID
GROUP BY s.AssetID, m.GameTitle, m.LocationZone, CAST(s.StartTime AS DATE);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CasinoDB.Gold.FloorPerformance AS
SELECT 
    LocationZone,
    COUNT(DISTINCT AssetID) AS MachineCount,
    SUM(TotalCoinIn) AS ZoneCoinIn,
    SUM(NetWin) AS ZoneRevenue,
    (SUM(NetWin) / SUM(TotalCoinIn)) * 100 AS HoldPercentage
FROM CasinoDB.Silver.MachineDailyStats
GROUP BY LocationZone;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which LocationZone has the highest HoldPercentage in CasinoDB.Gold.FloorPerformance?"
*/
