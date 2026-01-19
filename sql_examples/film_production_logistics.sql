/*
 * Film & TV Production Logistics Demo
 * 
 * Scenario:
 * A production studio tracks daily shooting schedules, scene completion, and expenses
 * to ensure the movie stays on budget and on schedule.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Production_Schools: Daily call sheets/schedules.
 * - Scenes: Metadata about script scenes (Int/Ext, Cast, PageCount).
 * - Expenses: Line item costs (Catering, OT, Equipment).
 * 
 * Silver Layer:
 * - Daily_Burn_Rate: Daily spend vs budget.
 * - Shooting_Progress: Pages shot vs Schedule.
 * 
 * Gold Layer:
 * - Production_Dashboard: Executive view of Budget % Used and Schedule Variance.
 * - Cast_Overtime_Analysis: Identifying expensive scheduling blockers.
 * 
 * Note: Assumes a catalog named 'FilmDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FilmDB;
CREATE FOLDER IF NOT EXISTS FilmDB.Bronze;
CREATE FOLDER IF NOT EXISTS FilmDB.Silver;
CREATE FOLDER IF NOT EXISTS FilmDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS FilmDB.Bronze.Scenes (
    SceneID VARCHAR,
    ScriptPageStart INT,
    ScriptPageEnd INT,
    LocationType VARCHAR, -- INT/EXT
    Setting VARCHAR, -- Kitchen, Park, SpaceStation
    EstShootingTimeHours DOUBLE
);

CREATE TABLE IF NOT EXISTS FilmDB.Bronze.Production_Schedule (
    DayID INT,
    Date DATE,
    SceneID VARCHAR,
    Status VARCHAR, -- Planned, Shot, Reshoot
    ActualShootingTimeHours DOUBLE
);

CREATE TABLE IF NOT EXISTS FilmDB.Bronze.Expenses (
    ExpenseID INT,
    DayID INT,
    Category VARCHAR, -- Cast, Crew, Equipment, Locations, Catering
    AmountUSD DOUBLE,
    ApprovalStatus VARCHAR
);

-- 1.2 Populate Bronze Tables

-- Insert 10 Scenes
INSERT INTO FilmDB.Bronze.Scenes (SceneID, ScriptPageStart, ScriptPageEnd, LocationType, Setting, EstShootingTimeHours) VALUES
('1A', 1, 3, 'INT', 'Hero Apartment', 4.0),
('2A', 4, 5, 'EXT', 'City Street', 6.0),
('2B', 6, 8, 'EXT', 'City Street Chase', 8.0),
('3A', 9, 12, 'INT', 'Villain Lair', 5.0),
('3B', 13, 14, 'INT', 'Villain Lair Fight', 10.0),
('4A', 15, 16, 'INT', 'Police Station', 3.0),
('5A', 17, 20, 'EXT', 'Park Bench', 2.0),
('6A', 21, 22, 'INT', 'Hero Apartment Night', 3.0),
('7A', 23, 25, 'EXT', 'Rooftop', 5.0),
('8A', 26, 30, 'INT', 'Hospital', 4.0);

-- Insert 50 records into FilmDB.Bronze.Production_Schedule (Scenes assigned to days)
-- Repeating scenes for multi-day shoots or multiple scenes per day
INSERT INTO FilmDB.Bronze.Production_Schedule (DayID, Date, SceneID, Status, ActualShootingTimeHours) VALUES
(1, '2025-05-01', '1A', 'Shot', 4.5), -- Over
(1, '2025-05-01', '6A', 'Shot', 2.5), -- Under
(2, '2025-05-02', '2A', 'Shot', 7.0), -- Rain delay
(3, '2025-05-03', '2B', 'Shot', 8.5),
(4, '2025-05-04', '3A', 'Shot', 5.0),
(5, '2025-05-05', '3B', 'Shot', 6.0), -- Part 1
(5, '2025-05-05', '4A', 'Shot', 3.5),
(6, '2025-05-06', '3B', 'Shot', 5.0), -- Part 2
(7, '2025-05-07', '5A', 'Shot', 2.0),
(8, '2025-05-08', '7A', 'Shot', 6.0),
(9, '2025-05-09', '8A', 'Shot', 4.5),
(10, '2025-05-10', '1A', 'Reshoot', 2.0), -- Pickup shot
-- Future planned days
(11, '2025-05-12', '2A', 'Planned', NULL),
(12, '2025-05-13', '2B', 'Planned', NULL),
(13, '2025-05-14', '3A', 'Planned', NULL),
(14, '2025-05-15', '3B', 'Planned', NULL),
(15, '2025-05-16', '4A', 'Planned', NULL),
(16, '2025-05-19', '5A', 'Planned', NULL),
(17, '2025-05-20', '6A', 'Planned', NULL),
(18, '2025-05-21', '7A', 'Planned', NULL),
(19, '2025-05-22', '8A', 'Planned', NULL),
(20, '2025-05-23', '1A', 'Planned', NULL),
(21, '2025-05-26', '2A', 'Planned', NULL),
(22, '2025-05-27', '2B', 'Planned', NULL),
(23, '2025-05-28', '3A', 'Planned', NULL),
(24, '2025-05-29', '3B', 'Planned', NULL),
(25, '2025-05-30', '4A', 'Planned', NULL),
(26, '2025-06-02', '5A', 'Planned', NULL),
(27, '2025-06-03', '6A', 'Planned', NULL),
(28, '2025-06-04', '7A', 'Planned', NULL),
(29, '2025-06-05', '8A', 'Planned', NULL),
(30, '2025-06-06', '1A', 'Planned', NULL),
(31, '2025-06-09', '2A', 'Planned', NULL),
(32, '2025-06-10', '2B', 'Planned', NULL),
(33, '2025-06-11', '3A', 'Planned', NULL),
(34, '2025-06-12', '3B', 'Planned', NULL),
(35, '2025-06-13', '4A', 'Planned', NULL),
(36, '2025-06-16', '5A', 'Planned', NULL),
(37, '2025-06-17', '6A', 'Planned', NULL),
(38, '2025-06-18', '7A', 'Planned', NULL),
(39, '2025-06-19', '8A', 'Planned', NULL),
(40, '2025-06-20', '1A', 'Planned', NULL),
(41, '2025-06-23', '2A', 'Planned', NULL),
(42, '2025-06-24', '2B', 'Planned', NULL),
(43, '2025-06-25', '3A', 'Planned', NULL),
(44, '2025-06-26', '3B', 'Planned', NULL),
(45, '2025-06-27', '4A', 'Planned', NULL),
(46, '2025-06-30', '5A', 'Planned', NULL),
(47, '2025-07-01', '6A', 'Planned', NULL),
(48, '2025-07-02', '7A', 'Planned', NULL),
(49, '2025-07-03', '8A', 'Planned', NULL),
(50, '2025-07-04', '1A', 'Planned', NULL);

-- Insert 50 records into FilmDB.Bronze.Expenses
INSERT INTO FilmDB.Bronze.Expenses (ExpenseID, DayID, Category, AmountUSD, ApprovalStatus) VALUES
(1, 1, 'Catering', 1500.0, 'Approved'),
(2, 1, 'Crew', 25000.0, 'Approved'),
(3, 1, 'Equipment', 5000.0, 'Approved'),
(4, 1, 'Cast OT', 2000.0, 'Approved'), -- Overtime
(5, 2, 'Catering', 1600.0, 'Approved'),
(6, 2, 'Crew', 25000.0, 'Approved'),
(7, 2, 'Locations', 10000.0, 'Approved'), -- Permit fees
(8, 3, 'Special FX', 15000.0, 'Approved'), -- Explosion
(9, 3, 'Catering', 2000.0, 'Approved'),
(10, 3, 'Crew', 30000.0, 'Approved'),
(11, 4, 'Catering', 1500.0, 'Approved'),
(12, 4, 'Crew', 25000.0, 'Approved'),
(13, 5, 'Stunts', 12000.0, 'Approved'),
(14, 5, 'Catering', 2200.0, 'Approved'),
(15, 6, 'Crew', 25000.0, 'Approved'),
(16, 6, 'Equipment', 8000.0, 'Approved'), -- Steadicam rental
(17, 7, 'Catering', 1000.0, 'Approved'),
(18, 8, 'Crew', 28000.0, 'Approved'), -- Night shoot premium
(19, 8, 'Lighting', 5000.0, 'Approved'),
(20, 9, 'Catering', 1500.0, 'Approved'),
(21, 9, 'Location', 5000.0, 'Approved'),
(22, 10, 'Catering', 800.0, 'Approved'),
(23, 10, 'Crew', 12000.0, 'Approved'), -- Half day
(24, 1, 'Costumes', 500.0, 'Approved'),
(25, 2, 'Transport', 1200.0, 'Approved'),
(26, 3, 'Transport', 1500.0, 'Approved'),
(27, 4, 'Makeup', 800.0, 'Approved'),
(28, 5, 'Makeup', 1000.0, 'Approved'),
(29, 6, 'Prop Rental', 3000.0, 'Approved'),
(30, 7, 'Animals', 500.0, 'Approved'), -- Pigeon wrangler
(31, 8, 'Police', 2000.0, 'Approved'), -- Traffic control
(32, 9, 'Medical', 1000.0, 'Approved'),
(33, 10, 'Props', 200.0, 'Approved'),
(34, 1, 'Misc', 100.0, 'Approved'),
(35, 2, 'Misc', 150.0, 'Approved'),
(36, 1, 'Cast', 50000.0, 'Approved'), -- Daily rate
(37, 2, 'Cast', 50000.0, 'Approved'),
(38, 3, 'Cast', 50000.0, 'Approved'),
(39, 4, 'Cast', 50000.0, 'Approved'),
(40, 5, 'Cast', 60000.0, 'Approved'), -- More actors
(41, 6, 'Cast', 50000.0, 'Approved'),
(42, 7, 'Cast', 50000.0, 'Approved'),
(43, 8, 'Cast', 50000.0, 'Approved'),
(44, 9, 'Cast', 50000.0, 'Approved'),
(45, 10, 'Cast', 25000.0, 'Approved'),
(46, 1, 'Insurance', 500.0, 'Approved'),
(47, 5, 'Insurance', 500.0, 'Approved'),
(48, 8, 'Insurance', 500.0, 'Approved'),
(49, 2, 'Damage', 1000.0, 'Pending'), -- Broken light
(50, 6, 'Damage', 500.0, 'Pending');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Tracking & Analysis
-------------------------------------------------------------------------------

-- 2.1 Daily Burn Rate
CREATE OR REPLACE VIEW FilmDB.Silver.Daily_Burn_Rate AS
SELECT
    DayID,
    SUM(AmountUSD) AS TotalSpend,
    SUM(CASE WHEN Category = 'Cast OT' OR Category LIKE '%Damage%' THEN AmountUSD ELSE 0 END) AS UnplannedSpend
FROM FilmDB.Bronze.Expenses
GROUP BY DayID;

-- 2.2 Shooting Progress Enriched
CREATE OR REPLACE VIEW FilmDB.Silver.Shooting_Progress AS
SELECT
    ps.Date,
    ps.DayID,
    ps.SceneID,
    s.ScriptPageEnd - s.ScriptPageStart + 1 AS PageCount,
    ps.ActualShootingTimeHours,
    s.EstShootingTimeHours,
    (ps.ActualShootingTimeHours - s.EstShootingTimeHours) AS VarianceHours
FROM FilmDB.Bronze.Production_Schedule ps
JOIN FilmDB.Bronze.Scenes s ON ps.SceneID = s.SceneID
WHERE ps.Status IN ('Shot', 'Reshoot');

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Executive Reporting
-------------------------------------------------------------------------------

-- 3.1 Production Dashboard
CREATE OR REPLACE VIEW FilmDB.Gold.Production_Dashboard AS
SELECT
    MAX(Date) AS ReportDate,
    SUM(PageCount) AS TotalPagesShot,
    SUM(VarianceHours) AS TotalScheduleVarianceHours, -- Positive means behind schedule
    (SELECT SUM(TotalSpend) FROM FilmDB.Silver.Daily_Burn_Rate) AS TotalSpendToDate
FROM FilmDB.Silver.Shooting_Progress;

-- 3.2 Expensive Scenes
CREATE OR REPLACE VIEW FilmDB.Gold.Scene_Cost_Variance AS
SELECT
    sp.SceneID,
    SUM(dbr.TotalSpend) AS SceneCost, -- Approx mapping day to scene
    AVG(sp.VarianceHours) AS AvgDelay
FROM FilmDB.Silver.Shooting_Progress sp
JOIN FilmDB.Silver.Daily_Burn_Rate dbr ON sp.DayID = dbr.DayID
GROUP BY sp.SceneID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Over Budget):
"Which DayID had the highest UnplannedSpend in FilmDB.Silver.Daily_Burn_Rate?"

PROMPT 2 (Schedule):
"Calculate the TotalScheduleVarianceHours from FilmDB.Gold.Production_Dashboard. are we ahead or behind?"

PROMPT 3 (Scene Costs):
"Which SceneID corresponds to the highest SceneCost in FilmDB.Gold.Scene_Cost_Variance?"
*/
