/*
 * Branch Staffing Optimization Demo
 * 
 * Scenario:
 * A retail bank wants to match teller schedules with peak customer traffic hours.
 * Uses door counter data (Footfall) and transaction logs.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Recommend optimal staffing levels per hour.
 * 
 * Note: Assumes a catalog named 'BranchOpsDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS BranchOpsDB;
CREATE FOLDER IF NOT EXISTS BranchOpsDB.Bronze;
CREATE FOLDER IF NOT EXISTS BranchOpsDB.Silver;
CREATE FOLDER IF NOT EXISTS BranchOpsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Operations Data
-------------------------------------------------------------------------------
-- Description: IoT Door counters and core banking transaction logs.

CREATE TABLE IF NOT EXISTS BranchOpsDB.Bronze.BranchVisits (
    VisitID INT,
    BranchID VARCHAR,
    EntryTime TIMESTAMP,
    ExitTime TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BranchOpsDB.Bronze.ServiceTransactions (
    TxnID INT,
    BranchID VARCHAR,
    TellerID VARCHAR,
    TxnTime TIMESTAMP,
    DurationsSeconds INT
);

-- 1.1 Populate BranchVisits (50+ Records)
-- Simulating a busy morning at Branch B-001
INSERT INTO BranchOpsDB.Bronze.BranchVisits (VisitID, BranchID, EntryTime, ExitTime) VALUES
(1, 'B-001', TIMESTAMP '2025-01-20 09:05:00', TIMESTAMP '2025-01-20 09:15:00'),
(2, 'B-001', TIMESTAMP '2025-01-20 09:10:00', TIMESTAMP '2025-01-20 09:20:00'),
(3, 'B-001', TIMESTAMP '2025-01-20 09:12:00', TIMESTAMP '2025-01-20 09:25:00'),
(4, 'B-001', TIMESTAMP '2025-01-20 09:15:00', TIMESTAMP '2025-01-20 09:45:00'),
(5, 'B-001', TIMESTAMP '2025-01-20 09:20:00', TIMESTAMP '2025-01-20 09:30:00'),
(6, 'B-001', TIMESTAMP '2025-01-20 09:30:00', TIMESTAMP '2025-01-20 09:40:00'),
(7, 'B-001', TIMESTAMP '2025-01-20 09:35:00', TIMESTAMP '2025-01-20 09:50:00'),
(8, 'B-001', TIMESTAMP '2025-01-20 09:40:00', TIMESTAMP '2025-01-20 09:55:00'),
(9, 'B-001', TIMESTAMP '2025-01-20 09:45:00', TIMESTAMP '2025-01-20 10:00:00'),
(10, 'B-001', TIMESTAMP '2025-01-20 09:50:00', TIMESTAMP '2025-01-20 10:10:00'),
(11, 'B-001', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 10:20:00'),
(12, 'B-001', TIMESTAMP '2025-01-20 10:05:00', TIMESTAMP '2025-01-20 10:15:00'),
(13, 'B-001', TIMESTAMP '2025-01-20 10:10:00', TIMESTAMP '2025-01-20 10:30:00'),
(14, 'B-001', TIMESTAMP '2025-01-20 10:15:00', TIMESTAMP '2025-01-20 10:35:00'),
(15, 'B-001', TIMESTAMP '2025-01-20 10:20:00', TIMESTAMP '2025-01-20 10:40:00'),
(16, 'B-001', TIMESTAMP '2025-01-20 10:25:00', TIMESTAMP '2025-01-20 10:45:00'),
(17, 'B-001', TIMESTAMP '2025-01-20 10:30:00', TIMESTAMP '2025-01-20 10:50:00'),
(18, 'B-001', TIMESTAMP '2025-01-20 10:35:00', TIMESTAMP '2025-01-20 10:55:00'),
(19, 'B-001', TIMESTAMP '2025-01-20 10:40:00', TIMESTAMP '2025-01-20 11:00:00'),
(20, 'B-001', TIMESTAMP '2025-01-20 10:45:00', TIMESTAMP '2025-01-20 11:05:00'),
(21, 'B-001', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 11:20:00'),
(22, 'B-001', TIMESTAMP '2025-01-20 11:05:00', TIMESTAMP '2025-01-20 11:25:00'),
(23, 'B-001', TIMESTAMP '2025-01-20 11:10:00', TIMESTAMP '2025-01-20 11:30:00'),
(24, 'B-001', TIMESTAMP '2025-01-20 11:15:00', TIMESTAMP '2025-01-20 11:35:00'),
(25, 'B-001', TIMESTAMP '2025-01-20 11:20:00', TIMESTAMP '2025-01-20 11:40:00'), -- Lull
(26, 'B-002', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:10:00'),
(27, 'B-002', TIMESTAMP '2025-01-20 09:10:00', TIMESTAMP '2025-01-20 09:20:00'),
(28, 'B-002', TIMESTAMP '2025-01-20 09:20:00', TIMESTAMP '2025-01-20 09:30:00'),
(29, 'B-002', TIMESTAMP '2025-01-20 09:30:00', TIMESTAMP '2025-01-20 09:40:00'),
(30, 'B-002', TIMESTAMP '2025-01-20 09:40:00', TIMESTAMP '2025-01-20 09:50:00'),
(31, 'B-001', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 12:20:00'), -- Lunch Rush
(32, 'B-001', TIMESTAMP '2025-01-20 12:05:00', TIMESTAMP '2025-01-20 12:25:00'),
(33, 'B-001', TIMESTAMP '2025-01-20 12:10:00', TIMESTAMP '2025-01-20 12:30:00'),
(34, 'B-001', TIMESTAMP '2025-01-20 12:15:00', TIMESTAMP '2025-01-20 12:35:00'),
(35, 'B-001', TIMESTAMP '2025-01-20 12:20:00', TIMESTAMP '2025-01-20 12:40:00'),
(36, 'B-001', TIMESTAMP '2025-01-20 12:25:00', TIMESTAMP '2025-01-20 12:45:00'),
(37, 'B-001', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 12:50:00'),
(38, 'B-001', TIMESTAMP '2025-01-20 12:35:00', TIMESTAMP '2025-01-20 12:55:00'),
(39, 'B-001', TIMESTAMP '2025-01-20 12:40:00', TIMESTAMP '2025-01-20 13:00:00'),
(40, 'B-001', TIMESTAMP '2025-01-20 12:45:00', TIMESTAMP '2025-01-20 13:05:00'),
(41, 'B-001', TIMESTAMP '2025-01-20 12:50:00', TIMESTAMP '2025-01-20 13:10:00'),
(42, 'B-001', TIMESTAMP '2025-01-20 12:55:00', TIMESTAMP '2025-01-20 13:15:00'),
(43, 'B-001', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 13:20:00'),
(44, 'B-001', TIMESTAMP '2025-01-20 13:05:00', TIMESTAMP '2025-01-20 13:25:00'),
(45, 'B-001', TIMESTAMP '2025-01-20 13:10:00', TIMESTAMP '2025-01-20 13:30:00'),
(46, 'B-001', TIMESTAMP '2025-01-20 13:15:00', TIMESTAMP '2025-01-20 13:35:00'),
(47, 'B-001', TIMESTAMP '2025-01-20 13:20:00', TIMESTAMP '2025-01-20 13:40:00'),
(48, 'B-001', TIMESTAMP '2025-01-20 13:25:00', TIMESTAMP '2025-01-20 13:45:00'),
(49, 'B-001', TIMESTAMP '2025-01-20 13:30:00', TIMESTAMP '2025-01-20 13:50:00'),
(50, 'B-001', TIMESTAMP '2025-01-20 13:35:00', TIMESTAMP '2025-01-20 13:55:00'),
(51, 'B-001', TIMESTAMP '2025-01-20 13:40:00', TIMESTAMP '2025-01-20 14:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Hourly Aggregation
-------------------------------------------------------------------------------
-- Description: Combining entry times into hourly buckets.
-- Feature: Counting unique visits per hour.

CREATE OR REPLACE VIEW BranchOpsDB.Silver.HourlyFootfallTrends AS
SELECT
    BranchID,
    EXTRACT(HOUR FROM EntryTime) AS HourOfDay,
    COUNT(VisitID) AS VisitorCount
FROM BranchOpsDB.Bronze.BranchVisits
GROUP BY BranchID, EXTRACT(HOUR FROM EntryTime);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Staffing Model
-------------------------------------------------------------------------------
-- Description: Calculating required staff.
-- Logic: 1 Teller per 5 visitors/hour.

CREATE OR REPLACE VIEW BranchOpsDB.Gold.StaffingRecommendations AS
SELECT
    BranchID,
    HourOfDay,
    VisitorCount,
    CEIL(VisitorCount / 5.0) AS RecommendedTellers
FROM BranchOpsDB.Silver.HourlyFootfallTrends;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Using BranchOpsDB.Gold.StaffingRecommendations, show the recommended tellers for Branch 'B-001' arranged by hour."

PROMPT:
"Which hour has the highest VisitorCount?"
*/
