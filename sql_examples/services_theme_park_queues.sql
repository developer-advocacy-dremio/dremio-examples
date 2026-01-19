/*
    Dremio High-Volume SQL Pattern: Services Theme Park Queues
    
    Business Scenario:
    Theme parks manage wait times to improve guest experience.
    We track "Queue Length" every 15 mins to find bottlenecks.
    
    Data Story:
    - Bronze: RideWaitLogs, RideMetadata.
    - Silver: WaitTimeTrends (Hourly Averages).
    - Gold: PeakCongestionReport.
    
    Medallion Architecture:
    - Bronze: RideWaitLogs, RideMetadata.
      *Volume*: 50+ records.
    - Silver: WaitTimeTrends.
    - Gold: PeakCongestionReport.
    
    Key Dremio Features:
    - EXTRACT(HOUR FROM ...)
    - AVG Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ServicesDB;
CREATE FOLDER IF NOT EXISTS ServicesDB.Bronze;
CREATE FOLDER IF NOT EXISTS ServicesDB.Silver;
CREATE FOLDER IF NOT EXISTS ServicesDB.Gold;
USE ServicesDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ServicesDB.Bronze.RideMetadata (
    RideID STRING,
    RideName STRING,
    Zone STRING
);

INSERT INTO ServicesDB.Bronze.RideMetadata VALUES
('R001', 'Space Coaster', 'SciFi'),
('R002', 'Jungle Cruise', 'Adventure'),
('R003', 'Haunted Manor', 'Spooky'),
('R004', 'Tea Cups', 'Fantasy');

CREATE OR REPLACE TABLE ServicesDB.Bronze.RideWaitLogs (
    LogTime TIMESTAMP,
    RideID STRING,
    WaitMinutes INT,
    FastPassWaitMinutes INT
);

INSERT INTO ServicesDB.Bronze.RideWaitLogs VALUES
-- Space Coaster (R001) - 9 AM to 12 PM
(TIMESTAMP '2025-01-20 09:00:00', 'R001', 10, 5),
(TIMESTAMP '2025-01-20 09:15:00', 'R001', 20, 5),
(TIMESTAMP '2025-01-20 09:30:00', 'R001', 30, 10),
(TIMESTAMP '2025-01-20 09:45:00', 'R001', 45, 10),
(TIMESTAMP '2025-01-20 10:00:00', 'R001', 60, 15),
(TIMESTAMP '2025-01-20 10:15:00', 'R001', 75, 15),
(TIMESTAMP '2025-01-20 10:30:00', 'R001', 90, 20),
(TIMESTAMP '2025-01-20 10:45:00', 'R001', 95, 25),
(TIMESTAMP '2025-01-20 11:00:00', 'R001', 100, 30),
(TIMESTAMP '2025-01-20 11:15:00', 'R001', 110, 30),
(TIMESTAMP '2025-01-20 11:30:00', 'R001', 120, 35),
(TIMESTAMP '2025-01-20 11:45:00', 'R001', 115, 30),

-- Jungle Cruise (R002)
(TIMESTAMP '2025-01-20 09:00:00', 'R002', 5, 0),
(TIMESTAMP '2025-01-20 09:15:00', 'R002', 10, 0),
(TIMESTAMP '2025-01-20 09:30:00', 'R002', 15, 5),
(TIMESTAMP '2025-01-20 09:45:00', 'R002', 20, 5),
(TIMESTAMP '2025-01-20 10:00:00', 'R002', 30, 10),
(TIMESTAMP '2025-01-20 10:15:00', 'R002', 35, 10),
(TIMESTAMP '2025-01-20 10:30:00', 'R002', 40, 15),
(TIMESTAMP '2025-01-20 10:45:00', 'R002', 45, 15),
(TIMESTAMP '2025-01-20 11:00:00', 'R002', 50, 15),
(TIMESTAMP '2025-01-20 11:15:00', 'R002', 55, 20),
(TIMESTAMP '2025-01-20 11:30:00', 'R002', 50, 15),
(TIMESTAMP '2025-01-20 11:45:00', 'R002', 45, 15),

-- Haunted Manor (R003)
(TIMESTAMP '2025-01-20 09:00:00', 'R003', 5, 0),
(TIMESTAMP '2025-01-20 09:15:00', 'R003', 5, 0),
(TIMESTAMP '2025-01-20 09:30:00', 'R003', 10, 0),
(TIMESTAMP '2025-01-20 09:45:00', 'R003', 15, 5),
(TIMESTAMP '2025-01-20 10:00:00', 'R003', 20, 5),
(TIMESTAMP '2025-01-20 10:15:00', 'R003', 25, 5),
(TIMESTAMP '2025-01-20 10:30:00', 'R003', 30, 5),
(TIMESTAMP '2025-01-20 10:45:00', 'R003', 35, 10),
(TIMESTAMP '2025-01-20 11:00:00', 'R003', 40, 10),
(TIMESTAMP '2025-01-20 11:15:00', 'R003', 45, 15),
(TIMESTAMP '2025-01-20 11:30:00', 'R003', 40, 10),
(TIMESTAMP '2025-01-20 11:45:00', 'R003', 35, 10),

-- Tea Cups (R004) - Always low
(TIMESTAMP '2025-01-20 09:00:00', 'R004', 0, 0),
(TIMESTAMP '2025-01-20 09:15:00', 'R004', 5, 0),
(TIMESTAMP '2025-01-20 09:30:00', 'R004', 5, 0),
(TIMESTAMP '2025-01-20 09:45:00', 'R004', 10, 0),
(TIMESTAMP '2025-01-20 10:00:00', 'R004', 10, 0),
(TIMESTAMP '2025-01-20 10:15:00', 'R004', 15, 0),
(TIMESTAMP '2025-01-20 10:30:00', 'R004', 15, 0),
(TIMESTAMP '2025-01-20 10:45:00', 'R004', 15, 0),
(TIMESTAMP '2025-01-20 11:00:00', 'R004', 10, 0),
(TIMESTAMP '2025-01-20 11:15:00', 'R004', 10, 0),
(TIMESTAMP '2025-01-20 11:30:00', 'R004', 5, 0),
(TIMESTAMP '2025-01-20 11:45:00', 'R004', 5, 0),

-- Some afternoon data to reach 50 records
(TIMESTAMP '2025-01-20 12:00:00', 'R001', 130, 40),
(TIMESTAMP '2025-01-20 12:15:00', 'R001', 125, 40),
(TIMESTAMP '2025-01-20 12:00:00', 'R002', 60, 20),
(TIMESTAMP '2025-01-20 12:15:00', 'R002', 55, 20),
(TIMESTAMP '2025-01-20 12:00:00', 'R003', 50, 15),
(TIMESTAMP '2025-01-20 12:30:00', 'R003', 45, 10);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Hourly Trends
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.WaitTimeTrends AS
SELECT
    r.RideName,
    r.Zone,
    EXTRACT(HOUR FROM l.LogTime) AS HourOfDay,
    AVG(l.WaitMinutes) AS AvgStandbyWait,
    AVG(l.FastPassWaitMinutes) AS AvgFastPassWait
FROM ServicesDB.Bronze.RideWaitLogs l
JOIN ServicesDB.Bronze.RideMetadata r ON l.RideID = r.RideID
GROUP BY r.RideName, r.Zone, EXTRACT(HOUR FROM l.LogTime);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Peak Congestion Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.PeakCongestionReport AS
SELECT
    RideName,
    HourOfDay,
    AvgStandbyWait,
    CASE 
        WHEN AvgStandbyWait > 60 THEN 'High Traffic'
        WHEN AvgStandbyWait > 30 THEN 'Moderate'
        ELSE 'Low'
    END AS CongestionLevel
FROM ServicesDB.Silver.WaitTimeTrends
WHERE AvgStandbyWait > 30
ORDER BY AvgStandbyWait DESC;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "When is the peak wait time for Space Coaster?"
    2. "List all rides with wait times over 60 minutes."
    3. "Average wait time by Theme Park Zone."
*/
