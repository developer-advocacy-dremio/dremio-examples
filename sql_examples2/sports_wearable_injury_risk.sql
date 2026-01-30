/*
 * Sports Analytics: Athlete Injury Risk Prevention
 * 
 * Scenario:
 * Correlating athlete training load (GPS/biometrics) with recovery scores to predict injury risk.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SportsScienceDB;
CREATE FOLDER IF NOT EXISTS SportsScienceDB.Performance;
CREATE FOLDER IF NOT EXISTS SportsScienceDB.Performance.Bronze;
CREATE FOLDER IF NOT EXISTS SportsScienceDB.Performance.Silver;
CREATE FOLDER IF NOT EXISTS SportsScienceDB.Performance.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Wearable Data
-------------------------------------------------------------------------------

-- TrainingSessions Table
CREATE TABLE IF NOT EXISTS SportsScienceDB.Performance.Bronze.TrainingSessions (
    SessionID VARCHAR,
    AthleteID VARCHAR,
    DistanceKm DOUBLE,
    SprintCount INT,
    HeartRateMax INT,
    TrainingLoadScore INT, -- 0-1000 arbitrary load units
    SessionDate DATE
);

INSERT INTO SportsScienceDB.Performance.Bronze.TrainingSessions VALUES
('S-001', 'A-01', 10.5, 15, 185, 600, '2026-09-01'),
('S-002', 'A-01', 5.0, 5, 150, 300, '2026-09-02'),
('S-003', 'A-02', 12.0, 20, 190, 800, '2026-09-01'), -- High load
('S-004', 'A-02', 12.0, 22, 192, 850, '2026-09-02'), -- Consecutive high load
('S-005', 'A-03', 8.0, 10, 170, 500, '2026-09-01'),
('S-006', 'A-04', 15.0, 30, 195, 950, '2026-09-01'), -- Danger
('S-007', 'A-04', 14.0, 28, 193, 900, '2026-09-02'), -- Overloaded
('S-008', 'A-05', 0.0, 0, 0, 0, '2026-09-01'), -- Rest
('S-009', 'A-06', 6.0, 8, 160, 400, '2026-09-01'),
('S-010', 'A-07', 7.0, 12, 175, 550, '2026-09-01'),
('S-011', 'A-08', 11.0, 18, 188, 700, '2026-09-01'),
('S-012', 'A-09', 9.0, 14, 180, 600, '2026-09-01'),
('S-013', 'A-10', 10.0, 16, 182, 650, '2026-09-01'),
('S-014', 'A-11', 13.0, 25, 199, 880, '2026-09-01'),
('S-015', 'A-12', 4.0, 2, 140, 200, '2026-09-01'),
('S-016', 'A-13', 5.0, 4, 145, 250, '2026-09-01'),
('S-017', 'A-14', 6.0, 6, 150, 300, '2026-09-01'),
('S-018', 'A-15', 8.0, 10, 170, 500, '2026-09-01'),
('S-019', 'A-16', 9.0, 12, 175, 550, '2026-09-01'),
('S-020', 'A-17', 10.0, 15, 180, 600, '2026-09-01'),
('S-021', 'A-18', 11.0, 18, 185, 650, '2026-09-01'),
('S-022', 'A-19', 12.0, 20, 190, 700, '2026-09-01'),
('S-023', 'A-20', 13.0, 22, 195, 750, '2026-09-01'),
('S-024', 'A-21', 14.0, 25, 198, 800, '2026-09-01'),
('S-025', 'A-22', 15.0, 30, 200, 900, '2026-09-01'),
('S-026', 'A-23', 5.0, 5, 150, 300, '2026-09-01'),
('S-027', 'A-24', 6.0, 6, 155, 350, '2026-09-01'),
('S-028', 'A-25', 7.0, 7, 160, 400, '2026-09-01'),
('S-029', 'A-01', 11.0, 16, 186, 620, '2026-09-03'),
('S-030', 'A-02', 13.0, 25, 195, 880, '2026-09-03'), -- Still hard
('S-031', 'A-04', 5.0, 2, 140, 250, '2026-09-03'), -- Tapered
('S-032', 'A-03', 9.0, 12, 175, 550, '2026-09-03'),
('S-033', 'A-05', 8.0, 10, 170, 500, '2026-09-03'),
('S-034', 'A-06', 7.0, 9, 165, 450, '2026-09-03'),
('S-035', 'A-07', 8.0, 11, 178, 560, '2026-09-03'),
('S-036', 'A-08', 12.0, 20, 190, 720, '2026-09-03'),
('S-037', 'A-09', 10.0, 15, 182, 620, '2026-09-03'),
('S-038', 'A-10', 11.0, 17, 185, 670, '2026-09-03'),
('S-039', 'A-11', 14.0, 28, 200, 910, '2026-09-03'), -- High
('S-040', 'A-12', 5.0, 3, 142, 220, '2026-09-03'),
('S-041', 'A-13', 6.0, 5, 148, 270, '2026-09-03'),
('S-042', 'A-14', 7.0, 7, 155, 320, '2026-09-03'),
('S-043', 'A-15', 9.0, 11, 172, 520, '2026-09-03'),
('S-044', 'A-16', 10.0, 13, 178, 570, '2026-09-03'),
('S-045', 'A-17', 11.0, 16, 182, 620, '2026-09-03'),
('S-046', 'A-18', 12.0, 19, 188, 670, '2026-09-03'),
('S-047', 'A-19', 13.0, 22, 192, 720, '2026-09-03'),
('S-048', 'A-20', 14.0, 24, 198, 770, '2026-09-03'),
('S-049', 'A-21', 15.0, 28, 200, 850, '2026-09-03'),
('S-050', 'A-22', 16.0, 32, 202, 950, '2026-09-03'); -- Breaking?

-- WellnessCheck Table
CREATE TABLE IF NOT EXISTS SportsScienceDB.Performance.Bronze.WellnessCheck (
    LogID VARCHAR,
    AthleteID VARCHAR,
    SleepHours DOUBLE,
    SorenessLevel INT, -- 1-10
    FatigueLevel INT, -- 1-10
    CheckDate DATE
);

INSERT INTO SportsScienceDB.Performance.Bronze.WellnessCheck VALUES
('W-001', 'A-01', 8.0, 3, 4, '2026-09-01'),
('W-002', 'A-02', 6.5, 7, 8, '2026-09-01'), -- Poor recovery
('W-003', 'A-03', 7.5, 4, 3, '2026-09-01'),
('W-004', 'A-04', 5.0, 8, 9, '2026-09-01'), -- Very bad
('W-005', 'A-05', 9.0, 1, 1, '2026-09-01'),
('W-006', 'A-06', 8.0, 3, 3, '2026-09-01'),
('W-007', 'A-07', 7.0, 4, 4, '2026-09-01'),
('W-008', 'A-08', 7.5, 5, 5, '2026-09-01'),
('W-009', 'A-09', 8.0, 3, 3, '2026-09-01'),
('W-010', 'A-10', 7.5, 4, 4, '2026-09-01'),
('W-011', 'A-11', 6.0, 7, 7, '2026-09-01'),
('W-012', 'A-12', 9.0, 2, 2, '2026-09-01'),
('W-013', 'A-13', 8.5, 2, 2, '2026-09-01'),
('W-014', 'A-14', 8.0, 3, 3, '2026-09-01'),
('W-015', 'A-15', 7.5, 4, 4, '2026-09-01'),
('W-016', 'A-16', 7.0, 4, 5, '2026-09-01'),
('W-017', 'A-17', 7.5, 3, 4, '2026-09-01'),
('W-018', 'A-18', 8.0, 3, 3, '2026-09-01'),
('W-019', 'A-19', 6.5, 6, 6, '2026-09-01'),
('W-020', 'A-20', 7.0, 5, 5, '2026-09-01'),
('W-021', 'A-21', 6.5, 6, 7, '2026-09-01'),
('W-022', 'A-22', 5.5, 8, 8, '2026-09-01'), -- Struggling
('W-023', 'A-23', 9.0, 1, 1, '2026-09-01'),
('W-024', 'A-24', 8.5, 2, 2, '2026-09-01'),
('W-025', 'A-25', 8.0, 3, 3, '2026-09-01'),
('W-026', 'A-02', 6.0, 8, 9, '2026-09-02'), -- Getting worse
('W-027', 'A-04', 9.0, 4, 5, '2026-09-02'), -- Recovering
('W-028', 'A-22', 5.0, 9, 9, '2026-09-02'), -- Critical
('W-029', 'A-11', 8.0, 5, 5, '2026-09-02'),
('W-030', 'A-01', 8.0, 4, 4, '2026-09-02');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Load vs Recovery
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SportsScienceDB.Performance.Silver.CheckReadiness AS
SELECT 
    t.AthleteID,
    t.SessionDate,
    SUM(t.TrainingLoadScore) AS DailyLoad,
    AVG(w.SleepHours) AS AvgSleep,
    MAX(w.SorenessLevel) AS MaxSoreness,
    MAX(w.FatigueLevel) AS MaxFatigue,
    -- Simple Readiness Score (0-100)
    (w.SleepHours * 10) - (w.SorenessLevel * 5) - (w.FatigueLevel * 5) AS ReadinessIndex
FROM SportsScienceDB.Performance.Bronze.TrainingSessions t
JOIN SportsScienceDB.Performance.Bronze.WellnessCheck w 
    ON t.AthleteID = w.AthleteID 
    AND t.SessionDate = w.CheckDate
GROUP BY t.AthleteID, t.SessionDate;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Injury Predictions
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SportsScienceDB.Performance.Gold.InjuryRiskMonitor AS
SELECT 
    AthleteID,
    SessionDate,
    DailyLoad,
    ReadinessIndex,
    -- ACWR (Acute:Chronic) proxy can be calculated here with window functions, simplified logic for now:
    CASE 
        WHEN DailyLoad > 800 AND ReadinessIndex < 40 THEN 'High Injury Risk'
        WHEN DailyLoad > 600 AND ReadinessIndex < 60 THEN 'Moderate Risk'
        ELSE 'Optimal Training Zone'
    END AS RiskAssessment
FROM SportsScienceDB.Performance.Silver.CheckReadiness;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all athletes tagged as 'High Injury Risk' in the Gold monitor."

PROMPT 2:
"Calculate the average sleep hours for athletes who ran more than 10km in a session."

PROMPT 3:
"Show the trend of ReadinessIndex for Athlete 'A-02' over time."
*/
