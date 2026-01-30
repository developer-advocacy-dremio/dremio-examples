/*
 * HealthTech: Digital Therapeutics (DTx) Adherence
 * 
 * Scenario:
 * Tracking daily active usage and adherence to prescribed digital therapeutic app modules (CBT-i, Diabetes Mgmt).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS DigitalHealthDB;
CREATE FOLDER IF NOT EXISTS DigitalHealthDB.Therapeutics;
CREATE FOLDER IF NOT EXISTS DigitalHealthDB.Therapeutics.Bronze;
CREATE FOLDER IF NOT EXISTS DigitalHealthDB.Therapeutics.Silver;
CREATE FOLDER IF NOT EXISTS DigitalHealthDB.Therapeutics.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: App Telemetry
-------------------------------------------------------------------------------

-- AppUsageLogs Table
CREATE TABLE IF NOT EXISTS DigitalHealthDB.Therapeutics.Bronze.AppUsageLogs (
    LogID INT,
    PatientID VARCHAR,
    AppModule VARCHAR, -- DailyLog, Meditation, CBT-Lesson, SymptomCheck
    DurationMinutes INT,
    CompletionStatus VARCHAR, -- Complete, Incomplete, Skipped
    LogTimestamp TIMESTAMP
);

INSERT INTO DigitalHealthDB.Therapeutics.Bronze.AppUsageLogs VALUES
(1, 'PT-001', 'DailyLog', 5, 'Complete', '2025-10-01 08:00:00'),
(2, 'PT-001', 'CBT-Lesson', 20, 'Complete', '2025-10-01 08:30:00'),
(3, 'PT-002', 'DailyLog', 2, 'Incomplete', '2025-10-01 09:00:00'),
(4, 'PT-003', 'Meditation', 15, 'Complete', '2025-10-01 07:00:00'),
(5, 'PT-003', 'SymptomCheck', 3, 'Complete', '2025-10-01 07:20:00'),
(6, 'PT-004', 'DailyLog', 0, 'Skipped', '2025-10-01 10:00:00'),
(7, 'PT-005', 'CBT-Lesson', 25, 'Complete', '2025-10-01 12:00:00'),
(8, 'PT-001', 'SymptomCheck', 4, 'Complete', '2025-10-02 08:00:00'),
(9, 'PT-001', 'DailyLog', 5, 'Complete', '2025-10-02 08:10:00'),
(10, 'PT-002', 'CBT-Lesson', 5, 'Incomplete', '2025-10-02 09:00:00'), -- Drop off
(11, 'PT-003', 'DailyLog', 5, 'Complete', '2025-10-02 07:00:00'),
(12, 'PT-004', 'DailyLog', 5, 'Complete', '2025-10-02 10:00:00'), -- Recovered
(13, 'PT-005', 'Meditation', 10, 'Complete', '2025-10-02 12:00:00'),
(14, 'PT-006', 'DailyLog', 5, 'Complete', '2025-10-01 08:00:00'),
(15, 'PT-007', 'CBT-Lesson', 30, 'Complete', '2025-10-01 09:00:00'),
(16, 'PT-008', 'SymptomCheck', 3, 'Complete', '2025-10-01 10:00:00'),
(17, 'PT-009', 'DailyLog', 5, 'Skipped', '2025-10-01 11:00:00'),
(18, 'PT-010', 'Meditation', 20, 'Complete', '2025-10-01 12:00:00'),
(19, 'PT-011', 'DailyLog', 5, 'Complete', '2025-10-01 13:00:00'),
(20, 'PT-012', 'CBT-Lesson', 15, 'Incomplete', '2025-10-01 14:00:00'), 
(21, 'PT-013', 'SymptomCheck', 2, 'Complete', '2025-10-01 15:00:00'),
(22, 'PT-014', 'DailyLog', 4, 'Complete', '2025-10-01 16:00:00'),
(23, 'PT-015', 'Meditation', 12, 'Complete', '2025-10-01 17:00:00'),
(24, 'PT-016', 'CBT-Lesson', 22, 'Complete', '2025-10-01 18:00:00'),
(25, 'PT-017', 'DailyLog', 6, 'Complete', '2025-10-01 19:00:00'),
(26, 'PT-018', 'SymptomCheck', 3, 'Skipped', '2025-10-01 20:00:00'),
(27, 'PT-019', 'Meditation', 15, 'Complete', '2025-10-01 21:00:00'),
(28, 'PT-020', 'DailyLog', 5, 'Complete', '2025-10-01 22:00:00'),
(29, 'PT-021', 'CBT-Lesson', 18, 'Complete', '2025-10-01 08:30:00'),
(30, 'PT-022', 'DailyLog', 2, 'Incomplete', '2025-10-01 09:30:00'),
(31, 'PT-023', 'SymptomCheck', 4, 'Complete', '2025-10-01 10:30:00'),
(32, 'PT-024', 'Meditation', 0, 'Skipped', '2025-10-01 11:30:00'),
(33, 'PT-025', 'DailyLog', 5, 'Complete', '2025-10-01 12:30:00'),
(34, 'PT-026', 'CBT-Lesson', 25, 'Complete', '2025-10-01 13:30:00'),
(35, 'PT-027', 'SymptomCheck', 3, 'Complete', '2025-10-01 14:30:00'),
(36, 'PT-028', 'DailyLog', 5, 'Skipped', '2025-10-01 15:30:00'),
(37, 'PT-029', 'Meditation', 10, 'Incomplete', '2025-10-01 16:30:00'),
(38, 'PT-030', 'DailyLog', 5, 'Complete', '2025-10-01 17:30:00'),
(39, 'PT-031', 'CBT-Lesson', 28, 'Complete', '2025-10-01 18:30:00'),
(40, 'PT-032', 'SymptomCheck', 3, 'Complete', '2025-10-01 19:30:00'),
(41, 'PT-033', 'DailyLog', 5, 'Complete', '2025-10-01 20:30:00'),
(42, 'PT-034', 'Meditation', 20, 'Complete', '2025-10-01 21:30:00'),
(43, 'PT-035', 'DailyLog', 5, 'Complete', '2025-10-02 08:00:00'),
(44, 'PT-036', 'CBT-Lesson', 5, 'Incomplete', '2025-10-02 09:00:00'),
(45, 'PT-037', 'SymptomCheck', 4, 'Complete', '2025-10-02 10:00:00'),
(46, 'PT-038', 'Meditation', 15, 'Complete', '2025-10-02 11:00:00'),
(47, 'PT-039', 'DailyLog', 5, 'Skipped', '2025-10-02 12:00:00'),
(48, 'PT-040', 'CBT-Lesson', 30, 'Complete', '2025-10-02 13:00:00'),
(49, 'PT-041', 'SymptomCheck', 3, 'Complete', '2025-10-02 14:00:00'),
(50, 'PT-042', 'DailyLog', 5, 'Complete', '2025-10-02 15:00:00');

-- PatientProfile Table
CREATE TABLE IF NOT EXISTS DigitalHealthDB.Therapeutics.Bronze.PatientProfile (
    PatientID VARCHAR,
    TherapyType VARCHAR, -- Insomnia, Anxiety, Depression
    StartDate DATE
);

INSERT INTO DigitalHealthDB.Therapeutics.Bronze.PatientProfile VALUES
('PT-001', 'Insomnia', '2025-09-01'),
('PT-002', 'Anxiety', '2025-09-15'),
('PT-003', 'Depression', '2025-09-20'),
('PT-004', 'Insomnia', '2025-09-25'),
('PT-005', 'Anxiety', '2025-09-28'),
('PT-006', 'Insomnia', '2025-09-02'),
('PT-007', 'Anxiety', '2025-09-03'),
('PT-008', 'Depression', '2025-09-04'),
('PT-009', 'Insomnia', '2025-09-05'),
('PT-010', 'Anxiety', '2025-09-06'),
('PT-011', 'Depression', '2025-09-07'),
('PT-012', 'Insomnia', '2025-09-08'),
('PT-013', 'Anxiety', '2025-09-09'),
('PT-014', 'Depression', '2025-09-10'),
('PT-015', 'Insomnia', '2025-09-11'),
('PT-016', 'Anxiety', '2025-09-12'),
('PT-017', 'Depression', '2025-09-13'),
('PT-018', 'Insomnia', '2025-09-14'),
('PT-019', 'Anxiety', '2025-09-16'),
('PT-020', 'Depression', '2025-09-17'),
('PT-021', 'Insomnia', '2025-09-18'),
('PT-022', 'Anxiety', '2025-09-19'),
('PT-023', 'Depression', '2025-09-21'),
('PT-024', 'Insomnia', '2025-09-22'),
('PT-025', 'Anxiety', '2025-09-23'),
('PT-026', 'Depression', '2025-09-24'),
('PT-027', 'Insomnia', '2025-09-26'),
('PT-028', 'Anxiety', '2025-09-27'),
('PT-029', 'Depression', '2025-09-29'),
('PT-030', 'Insomnia', '2025-09-30'),
('PT-031', 'Anxiety', '2025-10-01'),
('PT-032', 'Depression', '2025-10-01'),
('PT-033', 'Insomnia', '2025-10-01'),
('PT-034', 'Anxiety', '2025-10-01'),
('PT-035', 'Depression', '2025-10-01'),
('PT-036', 'Insomnia', '2025-10-01'),
('PT-037', 'Anxiety', '2025-10-01'),
('PT-038', 'Depression', '2025-10-01'),
('PT-039', 'Insomnia', '2025-10-01'),
('PT-040', 'Anxiety', '2025-10-01'),
('PT-041', 'Depression', '2025-10-01'),
('PT-042', 'Insomnia', '2025-10-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Compliance & Engagement
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW DigitalHealthDB.Therapeutics.Silver.PatientAdherence AS
SELECT 
    l.PatientID,
    p.TherapyType,
    l.AppModule,
    l.CompletionStatus,
    l.DurationMinutes,
    -- Basic Engagement Score: 10 pts for complete, 1 pt for incomplete
    CASE 
        WHEN l.CompletionStatus = 'Complete' THEN 10
        WHEN l.CompletionStatus = 'Incomplete' THEN 1
        ELSE 0
    END AS EngagementScore,
    l.LogTimestamp AS SessionTime
FROM DigitalHealthDB.Therapeutics.Bronze.AppUsageLogs l
JOIN DigitalHealthDB.Therapeutics.Bronze.PatientProfile p ON l.PatientID = p.PatientID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Therapy Outcomes
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW DigitalHealthDB.Therapeutics.Gold.AdherenceRisk AS
SELECT 
    PatientID,
    TherapyType,
    SUM(EngagementScore) AS TotalScore,
    COUNT(CASE WHEN CompletionStatus = 'Skipped' THEN 1 END) AS SkippedSessions,
    CASE 
        WHEN SUM(EngagementScore) < 5 THEN 'High Risk (Dropout)'
        WHEN COUNT(CASE WHEN CompletionStatus = 'Skipped' THEN 1 END) > 2 THEN 'Moderate Risk'
        ELSE 'On Track'
    END AS RiskLevel
FROM DigitalHealthDB.Therapeutics.Silver.PatientAdherence
GROUP BY PatientID, TherapyType;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify all patients classified as 'High Risk (Dropout)' in the Gold layer."

PROMPT 2:
"Calculate the average duration of 'Meditation' sessions for 'Anxiety' patients using the Silver layer."

PROMPT 3:
"List the completion rates for 'CBT-Lesson' modules across all patients."
*/
