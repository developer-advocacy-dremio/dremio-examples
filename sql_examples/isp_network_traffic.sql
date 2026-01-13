/*
 * ISP Network Traffic Demo
 * 
 * Scenario:
 * Analyzing bandwidth usage, latency, and packet loss across network nodes.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify congestion bottlenecks and plan capacity upgrades.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS TelcoNetDB;
CREATE FOLDER IF NOT EXISTS TelcoNetDB.Bronze;
CREATE FOLDER IF NOT EXISTS TelcoNetDB.Silver;
CREATE FOLDER IF NOT EXISTS TelcoNetDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS TelcoNetDB.Bronze.NodeTraffic (
    NodeID VARCHAR,
    Region VARCHAR,
    "Timestamp" TIMESTAMP,
    UploadMbps DOUBLE,
    DownloadMbps DOUBLE,
    PacketLossPct DOUBLE,
    LatencyMs INT
);

INSERT INTO TelcoNetDB.Bronze.NodeTraffic VALUES
('N-NYC-01', 'East', '2025-01-01 18:00:00', 450.0, 900.0, 0.1, 15),
('N-NYC-01', 'East', '2025-01-01 18:05:00', 500.0, 950.0, 0.5, 25), -- Congestion starting
('N-LA-01', 'West', '2025-01-01 18:00:00', 300.0, 600.0, 0.0, 10);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TelcoNetDB.Silver.HourlyNodeStats AS
SELECT 
    NodeID,
    Region,
    DATE_TRUNC('HOUR', "Timestamp") AS HourBlock,
    AVG(DownloadMbps) AS AvgDownload,
    MAX(DownloadMbps) AS PeakDownload,
    AVG(LatencyMs) AS AvgLatency,
    MAX(PacketLossPct) AS MaxPacketLoss
FROM TelcoNetDB.Bronze.NodeTraffic
GROUP BY NodeID, Region, DATE_TRUNC('HOUR', "Timestamp");

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TelcoNetDB.Gold.CongestionHotspots AS
SELECT 
    NodeID,
    Region,
    HourBlock,
    PeakDownload,
    CASE 
        WHEN PeakDownload > 900 OR AvgLatency > 50 THEN 'Critical'
        WHEN PeakDownload > 700 THEN 'Warning'
        ELSE 'Normal'
    END AS CongestionLevel
FROM TelcoNetDB.Silver.HourlyNodeStats;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all 'Critical' CongestionLevel records in TelcoNetDB.Gold.CongestionHotspots."
*/
