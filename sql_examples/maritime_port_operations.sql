/*
 * Maritime Port Operations Demo
 * 
 * Scenario:
 * Tracking vessel dwell times, berth occupancy, and container throughput.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Maritime;
CREATE FOLDER IF NOT EXISTS RetailDB.Maritime.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Maritime.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Maritime.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Maritime.Bronze.VesselCalls (
    CallID INT,
    VesselName VARCHAR,
    BerthID VARCHAR,
    Arrival TIMESTAMP,
    Departure TIMESTAMP,
    ContainersLoaded INT,
    ContainersUnloaded INT
);

INSERT INTO RetailDB.Maritime.Bronze.VesselCalls VALUES
(1, 'SeaStar', 'B1', '2025-01-01 06:00:00', '2025-01-02 18:00:00', 500, 400),
(2, 'OceanGiant', 'B2', '2025-01-01 08:00:00', '2025-01-03 08:00:00', 1200, 1500), -- Long stay
(3, 'BlueWave', 'B1', '2025-01-03 06:00:00', '2025-01-03 14:00:00', 200, 150),
(4, 'CargoKing', 'B3', '2025-01-02 10:00:00', '2025-01-02 22:00:00', 300, 300),
(5, 'PacificTrader', 'B2', '2025-01-03 10:00:00', '2025-01-04 10:00:00', 600, 600),
(6, 'AtlanticVoyager', 'B1', '2025-01-04 06:00:00', '2025-01-05 06:00:00', 800, 700),
(7, 'NorthernLight', 'B3', '2025-01-03 08:00:00', '2025-01-03 20:00:00', 250, 200),
(8, 'SouthernCross', 'B2', '2025-01-05 06:00:00', '2025-01-06 18:00:00', 900, 1000),
(9, 'EastWind', 'B1', '2025-01-06 06:00:00', '2025-01-06 18:00:00', 400, 400),
(10, 'WestBound', 'B3', '2025-01-04 10:00:00', '2025-01-05 10:00:00', 500, 500);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Maritime.Silver.BerthActivity AS
SELECT 
    VesselName,
    BerthID,
    Arrival,
    Departure,
    DATE_DIFF(CAST(Departure AS TIMESTAMP), CAST(Arrival AS TIMESTAMP)) AS DwellHours,
    (ContainersLoaded + ContainersUnloaded) AS TotalMoves
FROM RetailDB.Maritime.Bronze.VesselCalls;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Maritime.Gold.PortThroughput AS
SELECT 
    BerthID,
    COUNT(*) AS VesselCount,
    AVG(DwellHours) AS AvgDwellTime,
    SUM(TotalMoves) AS TotalContainersMoved,
    (SUM(TotalMoves) / SUM(DwellHours)) AS MovesPerHour
FROM RetailDB.Maritime.Silver.BerthActivity
GROUP BY BerthID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which BerthID has the highest MovesPerHour efficiency in RetailDB.Maritime.Gold.PortThroughput?"
*/
