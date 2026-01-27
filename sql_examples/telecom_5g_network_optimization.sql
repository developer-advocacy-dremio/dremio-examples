/*
 * Telecom 5G Network Optimization Demo
 * 
 * Scenario:
 * Monitoring cell tower latency, throughput, and hardware status for optimization.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS TelecomDB;
CREATE FOLDER IF NOT EXISTS TelecomDB.TelecomInfra;
CREATE FOLDER IF NOT EXISTS TelecomDB.TelecomInfra.Bronze;
CREATE FOLDER IF NOT EXISTS TelecomDB.TelecomInfra.Silver;
CREATE FOLDER IF NOT EXISTS TelecomDB.TelecomInfra.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS TelecomDB.TelecomInfra.Bronze.TowerLogs (
    TowerID VARCHAR,
    CellSiteID VARCHAR,
    LatencyMs INT,
    ThroughputMbps DOUBLE,
    PacketLossPct DOUBLE,
    LogTime TIMESTAMP,
    Status VARCHAR -- Active, Degraded, Down
);

INSERT INTO TelecomDB.TelecomInfra.Bronze.TowerLogs VALUES
('T100', 'C1', 15, 850.0, 0.1, '2025-01-01 10:00:00', 'Active'),
('T100', 'C2', 18, 700.0, 0.2, '2025-01-01 10:00:00', 'Active'),
('T101', 'C1', 45, 200.0, 2.5, '2025-01-01 10:00:00', 'Degraded'), -- High latency/loss
('T101', 'C2', 50, 150.0, 3.0, '2025-01-01 10:05:00', 'Degraded'),
('T102', 'C1', 12, 900.0, 0.0, '2025-01-01 10:00:00', 'Active'),
('T103', 'C1', 10, 950.0, 0.0, '2025-01-01 10:00:00', 'Active'),
('T104', 'C1', 0, 0.0, 100.0, '2025-01-01 10:00:00', 'Down'), -- Outage
('T100', 'C1', 16, 840.0, 0.1, '2025-01-01 10:05:00', 'Active'),
('T102', 'C2', 14, 880.0, 0.05, '2025-01-01 10:05:00', 'Active'),
('T104', 'C1', 0, 0.0, 100.0, '2025-01-01 10:05:00', 'Down'),
('T105', 'C1', 20, 600.0, 0.5, '2025-01-01 10:00:00', 'Active');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TelecomDB.TelecomInfra.Silver.NetworkHealth AS
SELECT 
    TowerID,
    CellSiteID,
    LogTime,
    LatencyMs,
    ThroughputMbps,
    PacketLossPct,
    Status
FROM TelecomDB.TelecomInfra.Bronze.TowerLogs;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TelecomDB.TelecomInfra.Gold.TowerPerformance AS
SELECT 
    TowerID,
    COUNT(*) AS LogCount,
    AVG(LatencyMs) AS AvgLatency,
    AVG(ThroughputMbps) AS AvgThroughput,
    MAX(PacketLossPct) AS MaxPacketLoss,
    SUM(CASE WHEN Status = 'Down' THEN 1 ELSE 0 END) AS DowntimeEvents
FROM TelecomDB.TelecomInfra.Silver.NetworkHealth
GROUP BY TowerID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Find all TowerIDs in TelecomDB.TelecomInfra.Gold.TowerPerformance that have any DowntimeEvents."
*/
