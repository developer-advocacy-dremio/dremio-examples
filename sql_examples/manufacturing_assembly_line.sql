/*
    Dremio High-Volume SQL Pattern: Manufacturing Assembly Line
    
    Business Scenario:
    Balancing an assembly line requires matching "Cycle Time" (actual time to work)
    with "Takt Time" (pace of customer demand). 
    Stations taking longer than Takt Time create bottlenecks.
    
    Data Story:
    - Bronze: StationLogs (Start/End times), ProductionSchedule (Takt Targets).
    - Silver: CycleTimeMetrics (Calculated Duration vs Target).
    - Gold: BottleneckAnalysis (Stations consistently exceeding Takt).
    
    Medallion Architecture:
    - Bronze: StationLogs, ProductionSchedule.
      *Volume*: 50+ records.
    - Silver: CycleTimeMetrics.
    - Gold: BottleneckAnalysis.
    
    Key Dremio Features:
    - TIMESTAMPDIFF (Second precision)
    - AVG / MAX aggregations
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ManufacturingDB;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Gold;
USE ManufacturingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ManufacturingDB.Bronze.ProductionSchedule (
    ShiftID STRING,
    Date DATE,
    TargetTaktTime_Secs INT -- e.g., 60 seconds per unit
);

INSERT INTO ManufacturingDB.Bronze.ProductionSchedule VALUES
('SHIFT-001', DATE '2025-01-20', 60);

CREATE OR REPLACE TABLE ManufacturingDB.Bronze.StationLogs (
    LogID INT,
    StationID STRING, -- S1, S2, S3, S4, S5
    UnitID STRING,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP
);

INSERT INTO ManufacturingDB.Bronze.StationLogs VALUES
-- S1: Fast (45-50s)
(1, 'S1', 'U001', TIMESTAMP '2025-01-20 08:00:00', TIMESTAMP '2025-01-20 08:00:45'),
(2, 'S1', 'U002', TIMESTAMP '2025-01-20 08:01:00', TIMESTAMP '2025-01-20 08:01:48'),
(3, 'S1', 'U003', TIMESTAMP '2025-01-20 08:02:00', TIMESTAMP '2025-01-20 08:02:50'),
(4, 'S1', 'U004', TIMESTAMP '2025-01-20 08:03:00', TIMESTAMP '2025-01-20 08:03:45'),
(5, 'S1', 'U005', TIMESTAMP '2025-01-20 08:04:00', TIMESTAMP '2025-01-20 08:04:46'),
(6, 'S1', 'U006', TIMESTAMP '2025-01-20 08:05:00', TIMESTAMP '2025-01-20 08:05:47'),
(7, 'S1', 'U007', TIMESTAMP '2025-01-20 08:06:00', TIMESTAMP '2025-01-20 08:06:44'),
(8, 'S1', 'U008', TIMESTAMP '2025-01-20 08:07:00', TIMESTAMP '2025-01-20 08:07:49'),
(9, 'S1', 'U009', TIMESTAMP '2025-01-20 08:08:00', TIMESTAMP '2025-01-20 08:08:45'),
(10, 'S1', 'U010', TIMESTAMP '2025-01-20 08:09:00', TIMESTAMP '2025-01-20 08:09:48'),

-- S2: Bottleneck (70-80s)
(11, 'S2', 'U001', TIMESTAMP '2025-01-20 08:01:00', TIMESTAMP '2025-01-20 08:02:15'), -- 75s
(12, 'S2', 'U002', TIMESTAMP '2025-01-20 08:02:30', TIMESTAMP '2025-01-20 08:03:50'), -- 80s
(13, 'S2', 'U003', TIMESTAMP '2025-01-20 08:04:00', TIMESTAMP '2025-01-20 08:05:10'), -- 70s
(14, 'S2', 'U004', TIMESTAMP '2025-01-20 08:05:30', TIMESTAMP '2025-01-20 08:06:55'), -- 85s
(15, 'S2', 'U005', TIMESTAMP '2025-01-20 08:07:10', TIMESTAMP '2025-01-20 08:08:25'), -- 75s
(16, 'S2', 'U006', TIMESTAMP '2025-01-20 08:08:40', TIMESTAMP '2025-01-20 08:09:55'), -- 75s
(17, 'S2', 'U007', TIMESTAMP '2025-01-20 08:10:10', TIMESTAMP '2025-01-20 08:11:30'), -- 80s
(18, 'S2', 'U008', TIMESTAMP '2025-01-20 08:11:45', TIMESTAMP '2025-01-20 08:13:00'), -- 75s
(19, 'S2', 'U009', TIMESTAMP '2025-01-20 08:13:15', TIMESTAMP '2025-01-20 08:14:35'), -- 80s
(20, 'S2', 'U010', TIMESTAMP '2025-01-20 08:14:50', TIMESTAMP '2025-01-20 08:16:05'), -- 75s

-- S3: Variable (50-70s)
(21, 'S3', 'U001', TIMESTAMP '2025-01-20 08:02:30', TIMESTAMP '2025-01-20 08:03:25'),
(22, 'S3', 'U002', TIMESTAMP '2025-01-20 08:04:00', TIMESTAMP '2025-01-20 08:05:10'),
(23, 'S3', 'U003', TIMESTAMP '2025-01-20 08:05:30', TIMESTAMP '2025-01-20 08:06:20'),
(24, 'S3', 'U004', TIMESTAMP '2025-01-20 08:07:00', TIMESTAMP '2025-01-20 08:08:12'),
(25, 'S3', 'U005', TIMESTAMP '2025-01-20 08:08:40', TIMESTAMP '2025-01-20 08:09:30'),
(26, 'S3', 'U006', TIMESTAMP '2025-01-20 08:10:00', TIMESTAMP '2025-01-20 08:10:55'),
(27, 'S3', 'U007', TIMESTAMP '2025-01-20 08:11:40', TIMESTAMP '2025-01-20 08:12:45'),
(28, 'S3', 'U008', TIMESTAMP '2025-01-20 08:13:10', TIMESTAMP '2025-01-20 08:14:05'),
(29, 'S3', 'U009', TIMESTAMP '2025-01-20 08:14:40', TIMESTAMP '2025-01-20 08:15:40'),
(30, 'S3', 'U010', TIMESTAMP '2025-01-20 08:16:10', TIMESTAMP '2025-01-20 08:17:05'),

-- S4: Underutilized (30-40s)
(31, 'S4', 'U001', TIMESTAMP '2025-01-20 08:03:30', TIMESTAMP '2025-01-20 08:04:05'),
(32, 'S4', 'U002', TIMESTAMP '2025-01-20 08:05:20', TIMESTAMP '2025-01-20 08:06:00'),
(33, 'S4', 'U003', TIMESTAMP '2025-01-20 08:06:30', TIMESTAMP '2025-01-20 08:07:05'),
(34, 'S4', 'U004', TIMESTAMP '2025-01-20 08:08:20', TIMESTAMP '2025-01-20 08:08:55'),
(35, 'S4', 'U005', TIMESTAMP '2025-01-20 08:09:40', TIMESTAMP '2025-01-20 08:10:15'),
(36, 'S4', 'U006', TIMESTAMP '2025-01-20 08:11:00', TIMESTAMP '2025-01-20 08:11:35'),
(37, 'S4', 'U007', TIMESTAMP '2025-01-20 08:13:00', TIMESTAMP '2025-01-20 08:13:35'),
(38, 'S4', 'U008', TIMESTAMP '2025-01-20 08:14:15', TIMESTAMP '2025-01-20 08:14:50'),
(39, 'S4', 'U009', TIMESTAMP '2025-01-20 08:15:50', TIMESTAMP '2025-01-20 08:16:25'),
(40, 'S4', 'U010', TIMESTAMP '2025-01-20 08:17:15', TIMESTAMP '2025-01-20 08:17:50'),

-- S5: Consistent (58-62s)
(41, 'S5', 'U001', TIMESTAMP '2025-01-20 08:04:10', TIMESTAMP '2025-01-20 08:05:10'),
(42, 'S5', 'U002', TIMESTAMP '2025-01-20 08:06:10', TIMESTAMP '2025-01-20 08:07:11'),
(43, 'S5', 'U003', TIMESTAMP '2025-01-20 08:07:15', TIMESTAMP '2025-01-20 08:08:15'),
(44, 'S5', 'U004', TIMESTAMP '2025-01-20 08:09:00', TIMESTAMP '2025-01-20 08:10:02'),
(45, 'S5', 'U005', TIMESTAMP '2025-01-20 08:10:20', TIMESTAMP '2025-01-20 08:11:20'),
(46, 'S5', 'U006', TIMESTAMP '2025-01-20 08:11:40', TIMESTAMP '2025-01-20 08:12:41'),
(47, 'S5', 'U007', TIMESTAMP '2025-01-20 08:13:40', TIMESTAMP '2025-01-20 08:14:40'),
(48, 'S5', 'U008', TIMESTAMP '2025-01-20 08:15:00', TIMESTAMP '2025-01-20 08:16:01'),
(49, 'S5', 'U009', TIMESTAMP '2025-01-20 08:16:30', TIMESTAMP '2025-01-20 08:17:30'),
(50, 'S5', 'U010', TIMESTAMP '2025-01-20 08:18:00', TIMESTAMP '2025-01-20 08:19:01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Cycle Time Metrics
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Silver.CycleTimeMetrics AS
SELECT
    l.StationID,
    l.UnitID,
    l.StartTime,
    l.EndTime,
    TIMESTAMPDIFF(SECOND, l.StartTime, l.EndTime) AS ActualCycleTime,
    s.TargetTaktTime_Secs,
    TIMESTAMPDIFF(SECOND, l.StartTime, l.EndTime) - s.TargetTaktTime_Secs AS Variances
FROM ManufacturingDB.Bronze.StationLogs l
JOIN ManufacturingDB.Bronze.ProductionSchedule s ON s.Date = DATE '2025-01-20';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Bottleneck Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Gold.BottleneckStations AS
SELECT
    StationID,
    COUNT(*) AS UnitsProcessed,
    AVG(ActualCycleTime) AS AvgCycleTime,
    TargetTaktTime_Secs,
    (AVG(ActualCycleTime) - TargetTaktTime_Secs) AS AvgVariance,
    CASE 
        WHEN AVG(ActualCycleTime) > (TargetTaktTime_Secs * 1.05) THEN 'BOTTLENECK'
        WHEN AVG(ActualCycleTime) < (TargetTaktTime_Secs * 0.70) THEN 'IDLE_CAPACITY'
        ELSE 'BALANCED'
    END AS Status
FROM ManufacturingDB.Silver.CycleTimeMetrics
GROUP BY StationID, TargetTaktTime_Secs;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Identify stations marked as 'BOTTLENECK' and their average cycle time."
    2. "Which station has the most idle capacity?"
    3. "Show the variance per unit for Station S2."
*/
