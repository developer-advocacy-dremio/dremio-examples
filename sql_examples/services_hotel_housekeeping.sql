/*
    Dremio High-Volume SQL Pattern: Services Hotel Housekeeping
    
    Business Scenario:
    Hotels track "Time to Clean" (TTC) to optimize maid staffing.
    We need to compare "Stayover" vs "Checkout" cleaning times and find inefficient floors.
    
    Data Story:
    - Bronze: RoomStatusLogs (Dirty -> clean timestamps), StaffRoster.
    - Silver: CleaningMetrics (Duration per room).
    - Gold: EfficiencyReport (Avg time per cleaner).
    
    Medallion Architecture:
    - Bronze: RoomStatusLogs, StaffRoster.
      *Volume*: 50+ records.
    - Silver: CleaningMetrics.
    - Gold: EfficiencyReport.
    
    Key Dremio Features:
    - TIMESTAMPDIFF (Minute)
    - AVG grouping
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
CREATE OR REPLACE TABLE ServicesDB.Bronze.StaffRoster (
    StaffID STRING,
    Name STRING,
    FloorAssigned INT
);

INSERT INTO ServicesDB.Bronze.StaffRoster VALUES
('S001', 'Alice', 1), ('S002', 'Bob', 2), ('S003', 'Charlie', 3),
('S004', 'David', 4), ('S005', 'Eva', 5);

CREATE OR REPLACE TABLE ServicesDB.Bronze.RoomStatusLogs (
    LogID INT,
    RoomID STRING,
    CleaningType STRING, -- Stayover, Checkout
    StaffID STRING,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP
);

INSERT INTO ServicesDB.Bronze.RoomStatusLogs VALUES
-- Floor 1 (Alice) - Efficient
(1, '101', 'Checkout', 'S001', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:30:00'), -- 30m
(2, '102', 'Stayover', 'S001', TIMESTAMP '2025-01-20 09:35:00', TIMESTAMP '2025-01-20 09:50:00'), -- 15m
(3, '103', 'Checkout', 'S001', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 10:35:00'), -- 35m
(4, '104', 'Stayover', 'S001', TIMESTAMP '2025-01-20 10:40:00', TIMESTAMP '2025-01-20 10:55:00'),
(5, '105', 'Checkout', 'S001', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 11:28:00'),
(6, '106', 'Stayover', 'S001', TIMESTAMP '2025-01-20 11:35:00', TIMESTAMP '2025-01-20 11:50:00'),
(7, '107', 'Checkout', 'S001', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 12:30:00'),
(8, '108', 'Stayover', 'S001', TIMESTAMP '2025-01-20 12:40:00', TIMESTAMP '2025-01-20 12:55:00'),
(9, '109', 'Checkout', 'S001', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 13:30:00'),
(10, '110', 'Stayover', 'S001', TIMESTAMP '2025-01-20 13:40:00', TIMESTAMP '2025-01-20 13:55:00'),

-- Floor 2 (Bob) - Slow
(11, '201', 'Checkout', 'S002', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:50:00'), -- 50m
(12, '202', 'Stayover', 'S002', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 10:30:00'), -- 30m
(13, '203', 'Checkout', 'S002', TIMESTAMP '2025-01-20 10:40:00', TIMESTAMP '2025-01-20 11:45:00'), -- 65m
(14, '204', 'Stayover', 'S002', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 12:35:00'),
(15, '205', 'Checkout', 'S002', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 13:55:00'),
(16, '206', 'Stayover', 'S002', TIMESTAMP '2025-01-20 14:00:00', TIMESTAMP '2025-01-20 14:30:00'),
(17, '207', 'Checkout', 'S002', TIMESTAMP '2025-01-20 14:40:00', TIMESTAMP '2025-01-20 15:40:00'),
(18, '208', 'Stayover', 'S002', TIMESTAMP '2025-01-20 15:50:00', TIMESTAMP '2025-01-20 16:20:00'),
(19, '209', 'Checkout', 'S002', TIMESTAMP '2025-01-20 16:30:00', TIMESTAMP '2025-01-20 17:30:00'),
(20, '210', 'Stayover', 'S002', TIMESTAMP '2025-01-20 17:40:00', TIMESTAMP '2025-01-20 18:10:00'),

-- Floor 3 (Charlie) - Mixed
(21, '301', 'Checkout', 'S003', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:40:00'),
(22, '302', 'Stayover', 'S003', TIMESTAMP '2025-01-20 09:50:00', TIMESTAMP '2025-01-20 10:10:00'),
(23, '303', 'Checkout', 'S003', TIMESTAMP '2025-01-20 10:20:00', TIMESTAMP '2025-01-20 11:00:00'),
(24, '304', 'Stayover', 'S003', TIMESTAMP '2025-01-20 11:10:00', TIMESTAMP '2025-01-20 11:30:00'),
(25, '305', 'Checkout', 'S003', TIMESTAMP '2025-01-20 11:40:00', TIMESTAMP '2025-01-20 12:20:00'),
(26, '306', 'Stayover', 'S003', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 12:50:00'),
(27, '307', 'Checkout', 'S003', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 13:40:00'),
(28, '308', 'Stayover', 'S003', TIMESTAMP '2025-01-20 13:50:00', TIMESTAMP '2025-01-20 14:10:00'),
(29, '309', 'Checkout', 'S003', TIMESTAMP '2025-01-20 14:20:00', TIMESTAMP '2025-01-20 15:00:00'),
(30, '310', 'Stayover', 'S003', TIMESTAMP '2025-01-20 15:10:00', TIMESTAMP '2025-01-20 15:30:00'),

-- Bulk filler for 4 & 5
(31, '401', 'Checkout', 'S004', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:35:00'),
(32, '402', 'Stayover', 'S004', TIMESTAMP '2025-01-20 09:40:00', TIMESTAMP '2025-01-20 10:00:00'),
(33, '403', 'Checkout', 'S004', TIMESTAMP '2025-01-20 10:10:00', TIMESTAMP '2025-01-20 10:45:00'),
(34, '404', 'Stayover', 'S004', TIMESTAMP '2025-01-20 10:50:00', TIMESTAMP '2025-01-20 11:10:00'),
(35, '405', 'Checkout', 'S004', TIMESTAMP '2025-01-20 11:20:00', TIMESTAMP '2025-01-20 11:55:00'),
(36, '406', 'Stayover', 'S004', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 12:20:00'),
(37, '407', 'Checkout', 'S004', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 13:05:00'),
(38, '408', 'Stayover', 'S004', TIMESTAMP '2025-01-20 13:10:00', TIMESTAMP '2025-01-20 13:30:00'),
(39, '409', 'Checkout', 'S004', TIMESTAMP '2025-01-20 13:40:00', TIMESTAMP '2025-01-20 14:15:00'),
(40, '410', 'Stayover', 'S004', TIMESTAMP '2025-01-20 14:20:00', TIMESTAMP '2025-01-20 14:40:00'),

(41, '501', 'Checkout', 'S005', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:30:00'),
(42, '502', 'Stayover', 'S005', TIMESTAMP '2025-01-20 09:35:00', TIMESTAMP '2025-01-20 09:50:00'),
(43, '503', 'Checkout', 'S005', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 10:30:00'),
(44, '504', 'Stayover', 'S005', TIMESTAMP '2025-01-20 10:40:00', TIMESTAMP '2025-01-20 10:55:00'),
(45, '505', 'Checkout', 'S005', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 11:30:00'),
(46, '506', 'Stayover', 'S005', TIMESTAMP '2025-01-20 11:40:00', TIMESTAMP '2025-01-20 11:55:00'),
(47, '507', 'Checkout', 'S005', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 12:30:00'),
(48, '508', 'Stayover', 'S005', TIMESTAMP '2025-01-20 12:40:00', TIMESTAMP '2025-01-20 12:55:00'),
(49, '509', 'Checkout', 'S005', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 13:30:00'),
(50, '510', 'Stayover', 'S005', TIMESTAMP '2025-01-20 13:40:00', TIMESTAMP '2025-01-20 13:55:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Cleaning Metrics
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.CleaningMetrics AS
SELECT
    l.LogID,
    l.RoomID,
    l.CleaningType,
    r.Name AS StaffName,
    r.FloorAssigned,
    TIMESTAMPDIFF(MINUTE, l.StartTime, l.EndTime) AS DurationMinutes
FROM ServicesDB.Bronze.RoomStatusLogs l
JOIN ServicesDB.Bronze.StaffRoster r ON l.StaffID = r.StaffID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Efficiency Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.EfficiencyReport AS
SELECT
    StaffName,
    CleaningType,
    COUNT(*) AS RoomsCleaned,
    AVG(DurationMinutes) AS AvgTimePerRoom,
    MIN(DurationMinutes) AS Fastest,
    MAX(DurationMinutes) AS Slowest
FROM ServicesDB.Silver.CleaningMetrics
GROUP BY StaffName, CleaningType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Who has the slowest average time for 'Checkout' cleaning?"
    2. "Compare cleaning times across Floors."
    3. "List all rooms that took longer than 60 minutes."
*/
