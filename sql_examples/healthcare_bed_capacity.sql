/*
 * Healthcare Bed Capacity Management Demo
 * 
 * Scenario:
 * Monitoring Emergency Room wait times and Inpatient bed occupancy to forecast capacity blocks.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareDB;
CREATE FOLDER IF NOT EXISTS HealthcareDB.BedMgmt;
CREATE FOLDER IF NOT EXISTS HealthcareDB.BedMgmt.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareDB.BedMgmt.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareDB.BedMgmt.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS HealthcareDB.BedMgmt.Bronze.BedStatus (
    BedID INT,
    Department VARCHAR, -- ICU, ER, General
    IsOccupied BOOLEAN,
    LastCleaned TIMESTAMP
);

INSERT INTO HealthcareDB.BedMgmt.Bronze.BedStatus VALUES
(1, 'ICU', true, '2025-01-01 10:00:00'),
(2, 'ICU', false, '2025-01-02 08:30:00'),
(3, 'ER', true, '2025-01-02 12:00:00'),
(4, 'ER', true, '2025-01-02 12:15:00'),
(5, 'ER', false, '2025-01-02 09:00:00'),
(6, 'General', true, '2025-01-01 15:00:00'),
(7, 'General', true, '2025-01-01 16:00:00'),
(8, 'General', false, '2025-01-02 11:00:00'),
(9, 'General', false, '2025-01-02 10:30:00'),
(10, 'General', true, '2025-01-02 09:45:00'),
(11, 'General', false, '2025-01-02 08:00:00'),
(12, 'ICU', true, '2025-01-02 01:00:00');

CREATE TABLE IF NOT EXISTS HealthcareDB.BedMgmt.Bronze.CheckIns (
    CheckInID INT,
    ExampleDepartment VARCHAR,
    CheckInTime TIMESTAMP,
    WaitTimeMinutes INT
);

INSERT INTO HealthcareDB.BedMgmt.Bronze.CheckIns VALUES
(1001, 'ER', '2025-01-02 10:00:00', 45),
(1002, 'ER', '2025-01-02 10:15:00', 30),
(1003, 'ER', '2025-01-02 10:30:00', 60),
(1004, 'General', '2025-01-02 08:00:00', 15),
(1005, 'General', '2025-01-02 09:00:00', 10),
(1006, 'ICU', '2025-01-02 01:00:00', 0),
(1007, 'ER', '2025-01-02 11:00:00', 90),
(1008, 'ER', '2025-01-02 11:30:00', 120),
(1009, 'General', '2025-01-02 12:00:00', 20),
(1010, 'General', '2025-01-02 13:00:00', 25);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HealthcareDB.BedMgmt.Silver.OccupancyMetrics AS
SELECT 
    Department,
    COUNT(BedID) AS TotalBeds,
    SUM(CASE WHEN IsOccupied THEN 1 ELSE 0 END) AS OccupiedBeds
FROM HealthcareDB.BedMgmt.Bronze.BedStatus
GROUP BY Department;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HealthcareDB.BedMgmt.Gold.CapacityForecast AS
SELECT 
    o.Department,
    o.TotalBeds,
    o.OccupiedBeds,
    (CAST(o.OccupiedBeds AS DOUBLE) / o.TotalBeds) * 100 AS OccupancyRate,
    AVG(c.WaitTimeMinutes) AS AvgWaitTime
FROM HealthcareDB.BedMgmt.Silver.OccupancyMetrics o
LEFT JOIN HealthcareDB.BedMgmt.Bronze.CheckIns c ON o.Department = c.ExampleDepartment
GROUP BY o.Department, o.TotalBeds, o.OccupiedBeds;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which department has the highest OccupancyRate in HealthcareDB.BedMgmt.Gold.CapacityForecast?"
*/
