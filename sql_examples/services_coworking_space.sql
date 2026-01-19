/*
    Dremio High-Volume SQL Pattern: Services Coworking Space
    
    Business Scenario:
    A coworking chain needs to track "Desk Usage" to optimize office layout.
    We track check-ins vs capacity to find peak hours and underutilized zones.
    
    Data Story:
    - Bronze: CheckInLogs, ZoneCapacity.
    - Silver: ZoneUsageMetrics (Hourly occupancy).
    - Gold: OfficeOptimization (Underutilized zones).
    
    Medallion Architecture:
    - Bronze: CheckInLogs, ZoneCapacity.
      *Volume*: 50+ records.
    - Silver: ZoneUsageMetrics.
    - Gold: OfficeOptimization.
    
    Key Dremio Features:
    - Join on ZoneID
    - Date Truncation
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
CREATE OR REPLACE TABLE ServicesDB.Bronze.ZoneCapacity (
    ZoneID STRING,
    ZoneName STRING, -- Quiet Area, Social Hub, Meeting Rooms
    MaxCapacity INT
);

INSERT INTO ServicesDB.Bronze.ZoneCapacity VALUES
('Z01', 'Quiet Area', 20),
('Z02', 'Social Hub', 50),
('Z03', 'Hot Desk Row A', 10),
('Z04', 'Hot Desk Row B', 10),
('Z05', 'Private Suites', 5);

CREATE OR REPLACE TABLE ServicesDB.Bronze.CheckInLogs (
    LogID INT,
    MemberID STRING,
    ZoneID STRING,
    CheckInTime TIMESTAMP,
    CheckOutTime TIMESTAMP
);

INSERT INTO ServicesDB.Bronze.CheckInLogs VALUES
-- Early Birds (Quiet Area)
(1, 'M001', 'Z01', TIMESTAMP '2025-01-20 08:00:00', TIMESTAMP '2025-01-20 12:00:00'),
(2, 'M002', 'Z01', TIMESTAMP '2025-01-20 08:15:00', TIMESTAMP '2025-01-20 11:00:00'),
(3, 'M003', 'Z01', TIMESTAMP '2025-01-20 08:30:00', TIMESTAMP '2025-01-20 17:00:00'),
(4, 'M004', 'Z01', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 10:00:00'),
(5, 'M005', 'Z01', TIMESTAMP '2025-01-20 09:15:00', TIMESTAMP '2025-01-20 13:00:00'),

-- Social Hub (Post-Lunch Rush)
(6, 'M006', 'Z02', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 14:00:00'),
(7, 'M007', 'Z02', TIMESTAMP '2025-01-20 12:10:00', TIMESTAMP '2025-01-20 13:30:00'),
(8, 'M008', 'Z02', TIMESTAMP '2025-01-20 12:15:00', TIMESTAMP '2025-01-20 14:30:00'),
(9, 'M009', 'Z02', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 15:00:00'),
(10, 'M010', 'Z02', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 16:00:00'),

-- Hot Desks (Consistent)
(11, 'M011', 'Z03', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 17:00:00'),
(12, 'M012', 'Z03', TIMESTAMP '2025-01-20 09:30:00', TIMESTAMP '2025-01-20 16:00:00'),
(13, 'M013', 'Z03', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 14:00:00'),
(14, 'M014', 'Z03', TIMESTAMP '2025-01-20 10:30:00', TIMESTAMP '2025-01-20 12:00:00'),
(15, 'M015', 'Z03', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 15:00:00'),

(16, 'M016', 'Z04', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 17:00:00'),
(17, 'M017', 'Z04', TIMESTAMP '2025-01-20 09:15:00', TIMESTAMP '2025-01-20 11:00:00'),
(18, 'M018', 'Z04', TIMESTAMP '2025-01-20 14:00:00', TIMESTAMP '2025-01-20 16:00:00'),
(19, 'M019', 'Z04', TIMESTAMP '2025-01-20 15:00:00', TIMESTAMP '2025-01-20 18:00:00'),
(20, 'M020', 'Z04', TIMESTAMP '2025-01-20 16:00:00', TIMESTAMP '2025-01-20 17:00:00'),

-- Bulk Data to 50
(21, 'M021', 'Z01', TIMESTAMP '2025-01-20 14:00:00', TIMESTAMP '2025-01-20 15:00:00'),
(22, 'M022', 'Z01', TIMESTAMP '2025-01-20 14:30:00', TIMESTAMP '2025-01-20 15:30:00'),
(23, 'M023', 'Z02', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 10:00:00'), -- Morning coffee
(24, 'M024', 'Z02', TIMESTAMP '2025-01-20 09:15:00', TIMESTAMP '2025-01-20 10:15:00'),
(25, 'M025', 'Z03', TIMESTAMP '2025-01-20 15:00:00', TIMESTAMP '2025-01-20 17:00:00'),
(26, 'M026', 'Z05', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 17:00:00'), -- Private suite full day
(27, 'M027', 'Z05', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 17:00:00'),
(28, 'M028', 'Z05', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 17:00:00'),
(29, 'M029', 'Z01', TIMESTAMP '2025-01-20 16:00:00', TIMESTAMP '2025-01-20 18:00:00'),
(30, 'M030', 'Z02', TIMESTAMP '2025-01-20 17:00:00', TIMESTAMP '2025-01-20 19:00:00'), -- Networking

(31, 'M031', 'Z02', TIMESTAMP '2025-01-20 17:15:00', TIMESTAMP '2025-01-20 19:00:00'),
(32, 'M032', 'Z03', TIMESTAMP '2025-01-20 08:30:00', TIMESTAMP '2025-01-20 10:00:00'),
(33, 'M033', 'Z03', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 13:00:00'),
(34, 'M034', 'Z04', TIMESTAMP '2025-01-20 11:30:00', TIMESTAMP '2025-01-20 12:30:00'),
(35, 'M035', 'Z04', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 13:30:00'),
(36, 'M036', 'Z01', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 12:00:00'),
(37, 'M037', 'Z01', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 13:00:00'),
(38, 'M038', 'Z01', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 15:00:00'),
(39, 'M039', 'Z02', TIMESTAMP '2025-01-20 14:00:00', TIMESTAMP '2025-01-20 16:00:00'),
(40, 'M040', 'Z02', TIMESTAMP '2025-01-20 15:00:00', TIMESTAMP '2025-01-20 17:00:00'),
(41, 'M041', 'Z03', TIMESTAMP '2025-01-20 16:00:00', TIMESTAMP '2025-01-20 18:00:00'),
(42, 'M042', 'Z03', TIMESTAMP '2025-01-20 17:00:00', TIMESTAMP '2025-01-20 18:00:00'),
(43, 'M043', 'Z04', TIMESTAMP '2025-01-20 08:00:00', TIMESTAMP '2025-01-20 09:00:00'),
(44, 'M044', 'Z04', TIMESTAMP '2025-01-20 18:00:00', TIMESTAMP '2025-01-20 19:00:00'),
(45, 'M045', 'Z05', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 17:00:00'),
(46, 'M046', 'Z01', TIMESTAMP '2025-01-20 18:00:00', TIMESTAMP '2025-01-20 20:00:00'),
(47, 'M047', 'Z01', TIMESTAMP '2025-01-20 18:30:00', TIMESTAMP '2025-01-20 20:00:00'),
(48, 'M048', 'Z02', TIMESTAMP '2025-01-20 19:00:00', TIMESTAMP '2025-01-20 21:00:00'), -- Evening event
(49, 'M049', 'Z02', TIMESTAMP '2025-01-20 19:15:00', TIMESTAMP '2025-01-20 21:00:00'),
(50, 'M050', 'Z02', TIMESTAMP '2025-01-20 19:30:00', TIMESTAMP '2025-01-20 21:00:00');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Zone Usage (Snapshop 10 AM)
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.ZoneUsageMetrics AS
SELECT
    z.ZoneName,
    z.MaxCapacity,
    COUNT(l.LogID) AS OccupantsAt10AM,
    COUNT(l.LogID) * 100.0 / z.MaxCapacity AS UtilizationPct
FROM ServicesDB.Bronze.ZoneCapacity z
LEFT JOIN ServicesDB.Bronze.CheckInLogs l 
    ON z.ZoneID = l.ZoneID
    AND TIMESTAMP '2025-01-20 10:00:00' BETWEEN l.CheckInTime AND l.CheckOutTime
GROUP BY z.ZoneName, z.MaxCapacity;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Office Optimization
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.OfficeOptimization AS
SELECT
    ZoneName,
    MaxCapacity,
    OccupantsAt10AM,
    UtilizationPct,
    CASE 
        WHEN UtilizationPct > 90 THEN 'EXPAND_CAPACITY'
        WHEN UtilizationPct < 20 THEN 'REPURPOSE_AREA'
        ELSE 'OPTIMAL'
    END AS Recommendation
FROM ServicesDB.Silver.ZoneUsageMetrics;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which zone is underutilized at 10 AM?"
    2. "List all zones needing expansion."
    3. "Show capacity vs actual usage."
*/
