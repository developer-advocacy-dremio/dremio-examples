/*
    Dremio High-Volume SQL Pattern: Lifestyle Fitness Tracker Trends
    
    Business Scenario:
    A wearable device company tracks user steps, heart rate, and sleep.
    We want to analyze "Goal Completion Rates" and correlating sleep with activity.
    
    Data Story:
    - Bronze: UserProfiles, DailyActivity (Steps/Sleep), HeartRateSamples.
    - Silver: DailySummary (Joined & Aggregated).
    - Gold: EngagementTrends (Active vs Churned users).
    
    Medallion Architecture:
    - Bronze: UserProfiles, DailyActivity.
      *Volume*: 50+ records.
    - Silver: DailySummary.
    - Gold: EngagementTrends.
    
    Key Dremio Features:
    - AVG / SUM aggregation
    - CASE statements for Segmentation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS LifestyleDB;
CREATE FOLDER IF NOT EXISTS LifestyleDB.Bronze;
CREATE FOLDER IF NOT EXISTS LifestyleDB.Silver;
CREATE FOLDER IF NOT EXISTS LifestyleDB.Gold;
USE LifestyleDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE LifestyleDB.Bronze.UserProfiles (
    UserID STRING,
    Region STRING, -- NA, EU, APAC
    AgeGroup STRING, -- 18-25, 26-35, 36-50, 50+
    StepGoal INT
);

INSERT INTO LifestyleDB.Bronze.UserProfiles VALUES
('U001', 'NA', '26-35', 10000), ('U002', 'NA', '18-25', 8000),
('U003', 'EU', '36-50', 10000), ('U004', 'APAC', '26-35', 12000),
('U005', 'NA', '50+', 5000), ('U006', 'EU', '18-25', 10000),
('U007', 'APAC', '36-50', 8000), ('U008', 'NA', '26-35', 15000),
('U009', 'EU', '50+', 6000), ('U010', 'APAC', '18-25', 10000);

CREATE OR REPLACE TABLE LifestyleDB.Bronze.DailyActivity (
    LogID INT,
    UserID STRING,
    Date DATE,
    StepsTaken INT,
    SleepMinutes INT, -- 480 = 8 hrs
    ActiveMinutes INT
);

-- Generating 50+ records for 10 users over 5 days
INSERT INTO LifestyleDB.Bronze.DailyActivity VALUES
-- User 1 (Consistent)
(1, 'U001', DATE '2025-01-01', 10500, 450, 60),
(2, 'U001', DATE '2025-01-02', 11000, 480, 65),
(3, 'U001', DATE '2025-01-03', 9500, 420, 55),
(4, 'U001', DATE '2025-01-04', 12000, 500, 70),
(5, 'U001', DATE '2025-01-05', 10200, 460, 60),

-- User 2 (Slacker)
(6, 'U002', DATE '2025-01-01', 4000, 550, 20),
(7, 'U002', DATE '2025-01-02', 3500, 600, 15),
(8, 'U002', DATE '2025-01-03', 4200, 500, 25),
(9, 'U002', DATE '2025-01-04', 3000, 620, 10),
(10, 'U002', DATE '2025-01-05', 4500, 580, 30),

-- User 3 (Average)
(11, 'U003', DATE '2025-01-01', 9800, 400, 45),
(12, 'U003', DATE '2025-01-02', 10100, 410, 50),
(13, 'U003', DATE '2025-01-03', 8900, 390, 40),
(14, 'U003', DATE '2025-01-04', 10500, 420, 55),
(15, 'U003', DATE '2025-01-05', 9200, 400, 45),

-- User 4 (Athlete)
(16, 'U004', DATE '2025-01-01', 15000, 480, 90),
(17, 'U004', DATE '2025-01-02', 16000, 490, 95),
(18, 'U004', DATE '2025-01-03', 14500, 470, 85),
(19, 'U004', DATE '2025-01-04', 17000, 500, 100),
(20, 'U004', DATE '2025-01-05', 15500, 480, 92),

-- User 5 (Senior)
(21, 'U005', DATE '2025-01-01', 5200, 420, 30),
(22, 'U005', DATE '2025-01-02', 5500, 430, 35),
(23, 'U005', DATE '2025-01-03', 5100, 410, 25),
(24, 'U005', DATE '2025-01-04', 5800, 440, 40),
(25, 'U005', DATE '2025-01-05', 5300, 420, 30),

-- User 6
(26, 'U006', DATE '2025-01-01', 11000, 460, 60),
(27, 'U006', DATE '2025-01-02', 9000, 400, 50),
(28, 'U006', DATE '2025-01-03', 12000, 500, 70),
(29, 'U006', DATE '2025-01-04', 8000, 380, 40),
(30, 'U006', DATE '2025-01-05', 13000, 520, 75),

-- User 7
(31, 'U007', DATE '2025-01-01', 7500, 450, 40),
(32, 'U007', DATE '2025-01-02', 7800, 460, 42),
(33, 'U007', DATE '2025-01-03', 7200, 440, 38),
(34, 'U007', DATE '2025-01-04', 8000, 470, 45),
(35, 'U007', DATE '2025-01-05', 7600, 450, 40),

-- User 8 (High Goal)
(36, 'U008', DATE '2025-01-01', 12000, 400, 60),
(37, 'U008', DATE '2025-01-02', 13000, 410, 65),
(38, 'U008', DATE '2025-01-03', 11000, 390, 55),
(39, 'U008', DATE '2025-01-04', 14000, 420, 70),
(40, 'U008', DATE '2025-01-05', 12500, 400, 62),

-- User 9
(41, 'U009', DATE '2025-01-01', 6000, 500, 30),
(42, 'U009', DATE '2025-01-02', 6200, 510, 32),
(43, 'U009', DATE '2025-01-03', 5800, 490, 28),
(44, 'U009', DATE '2025-01-04', 6500, 520, 35),
(45, 'U009', DATE '2025-01-05', 6100, 500, 30),

-- User 10
(46, 'U010', DATE '2025-01-01', 10500, 480, 60),
(47, 'U010', DATE '2025-01-02', 10800, 490, 62),
(48, 'U010', DATE '2025-01-03', 10200, 470, 58),
(49, 'U010', DATE '2025-01-04', 11000, 500, 65),
(50, 'U010', DATE '2025-01-05', 10600, 480, 60);


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Daily Summary
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW LifestyleDB.Silver.DailySummary AS
SELECT
    d.UserID,
    d.Date,
    p.Region,
    p.AgeGroup,
    d.StepsTaken,
    p.StepGoal,
    CASE WHEN d.StepsTaken >= p.StepGoal THEN 1 ELSE 0 END AS GoalMet,
    d.SleepMinutes / 60.0 AS SleepHours
FROM LifestyleDB.Bronze.DailyActivity d
JOIN LifestyleDB.Bronze.UserProfiles p ON d.UserID = p.UserID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Engagement Trends
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW LifestyleDB.Gold.EngagementTrends AS
SELECT
    AgeGroup,
    Region,
    AVG(StepsTaken) AS AvgSteps,
    AVG(SleepHours) AS AvgSleep,
    SUM(GoalMet) AS TotalGoalsHit,
    COUNT(*) AS TotalLogDays,
    (CAST(SUM(GoalMet) AS DOUBLE) / COUNT(*)) * 100.0 AS SuccessRatePct
FROM LifestyleDB.Silver.DailySummary
GROUP BY AgeGroup, Region;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Compare Steps Taken by Age Group."
    2. "Which Region has the highest Goal Completion Rate?"
    3. "Does more sleep correlate with higher steps?"
*/
