/*
 * EdTech: Adaptive Learning Paths
 * 
 * Scenario:
 * Real-time analysis of student quiz performance to dynamically adjust curriculum difficulty tiers.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EdTechDB;
CREATE FOLDER IF NOT EXISTS EdTechDB.Learning;
CREATE FOLDER IF NOT EXISTS EdTechDB.Learning.Bronze;
CREATE FOLDER IF NOT EXISTS EdTechDB.Learning.Silver;
CREATE FOLDER IF NOT EXISTS EdTechDB.Learning.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Quiz Telemetry
-------------------------------------------------------------------------------

-- QuizAttempts Table
CREATE TABLE IF NOT EXISTS EdTechDB.Learning.Bronze.QuizAttempts (
    AttemptID INT,
    StudentID VARCHAR,
    ModuleID VARCHAR, -- Algebra-101, Calc-202
    DifficultyTier INT, -- 1=Basic, 2=Intermediate, 3=Advanced
    ScorePercent DOUBLE,
    TimeTakenSeconds INT,
    AttemptTimestamp TIMESTAMP
);

INSERT INTO EdTechDB.Learning.Bronze.QuizAttempts VALUES
(1, 'S-100', 'Algebra-101', 1, 95.0, 300, '2025-03-01 10:00:00'), -- Aced it fast
(2, 'S-101', 'Algebra-101', 1, 60.0, 1200, '2025-03-01 10:00:00'), -- Struggled
(3, 'S-102', 'Algebra-101', 1, 85.0, 600, '2025-03-01 10:00:00'),
(4, 'S-100', 'Algebra-102', 2, 88.0, 450, '2025-03-02 10:00:00'), -- Moved up
(5, 'S-103', 'Calc-202', 3, 40.0, 1800, '2025-03-01 10:00:00'), -- Failed hard
(6, 'S-104', 'Algebra-101', 1, 75.0, 900, '2025-03-01 10:00:00'),
(7, 'S-105', 'Algebra-101', 1, 98.0, 250, '2025-03-01 10:00:00'), -- Genius?
(8, 'S-102', 'Algebra-102', 2, 70.0, 800, '2025-03-03 10:00:00'),
(9, 'S-106', 'Calc-202', 3, 92.0, 1500, '2025-03-01 10:00:00'),
(10, 'S-107', 'Algebra-101', 1, 55.0, 1300, '2025-03-01 10:00:00'), -- Needs help
(11, 'S-108', 'Algebra-101', 1, 80.0, 700, '2025-03-01 10:00:00'),
(12, 'S-109', 'Algebra-101', 1, 90.0, 320, '2025-03-01 10:00:00'),
(13, 'S-110', 'Algebra-101', 1, 65.0, 1100, '2025-03-01 10:00:00'),
(14, 'S-111', 'Calc-202', 3, 30.0, 1900, '2025-03-01 10:00:00'), -- Too hard
(15, 'S-112', 'Calc-202', 3, 85.0, 1600, '2025-03-01 10:00:00'),
(16, 'S-113', 'Algebra-101', 1, 100.0, 200, '2025-03-01 10:00:00'),
(17, 'S-114', 'Algebra-101', 1, 45.0, 1400, '2025-03-01 10:00:00'),
(18, 'S-115', 'Algebra-101', 1, 78.0, 850, '2025-03-01 10:00:00'),
(19, 'S-116', 'Algebra-101', 1, 35.0, 1500, '2025-03-01 10:00:00'), -- Struggling
(20, 'S-117', 'Algebra-101', 1, 94.0, 310, '2025-03-01 10:00:00'),
(21, 'S-118', 'Calc-202', 3, 50.0, 1750, '2025-03-01 10:00:00'),
(22, 'S-119', 'Calc-202', 3, 89.0, 1550, '2025-03-01 10:00:00'),
(23, 'S-120', 'Algebra-101', 1, 82.0, 750, '2025-03-01 10:00:00'),
(24, 'S-121', 'Algebra-101', 1, 58.0, 1250, '2025-03-01 10:00:00'),
(25, 'S-122', 'Algebra-101', 1, 96.0, 280, '2025-03-01 10:00:00'),
(26, 'S-123', 'Algebra-101', 1, 88.0, 400, '2025-03-01 10:00:00'),
(27, 'S-124', 'Algebra-101', 1, 62.0, 1150, '2025-03-01 10:00:00'),
(28, 'S-125', 'Algebra-101', 1, 40.0, 1450, '2025-03-01 10:00:00'),
(29, 'S-126', 'Calc-202', 3, 95.0, 1400, '2025-03-01 10:00:00'),
(30, 'S-127', 'Calc-202', 3, 35.0, 1850, '2025-03-01 10:00:00'),
(31, 'S-128', 'Algebra-101', 1, 72.0, 950, '2025-03-01 10:00:00'),
(32, 'S-129', 'Algebra-101', 1, 81.0, 780, '2025-03-01 10:00:00'),
(33, 'S-130', 'Algebra-101', 1, 50.0, 1350, '2025-03-01 10:00:00'),
(34, 'S-131', 'Algebra-101', 1, 91.0, 330, '2025-03-01 10:00:00'),
(35, 'S-132', 'Algebra-101', 1, 68.0, 1050, '2025-03-01 10:00:00'),
(36, 'S-133', 'Algebra-101', 1, 30.0, 1600, '2025-03-01 10:00:00'),
(37, 'S-134', 'Calc-202', 3, 98.0, 1300, '2025-03-01 10:00:00'),
(38, 'S-135', 'Calc-202', 3, 25.0, 1950, '2025-03-01 10:00:00'),
(39, 'S-136', 'Algebra-101', 1, 77.0, 880, '2025-03-01 10:00:00'),
(40, 'S-137', 'Algebra-101', 1, 85.0, 650, '2025-03-01 10:00:00'),
(41, 'S-138', 'Algebra-101', 1, 60.0, 1220, '2025-03-01 10:00:00'),
(42, 'S-139', 'Algebra-101', 1, 93.0, 300, '2025-03-01 10:00:00'),
(43, 'S-140', 'Algebra-101', 1, 38.0, 1480, '2025-03-01 10:00:00'),
(44, 'S-141', 'Algebra-101', 1, 70.0, 980, '2025-03-01 10:00:00'),
(45, 'S-142', 'Calc-202', 3, 90.0, 1450, '2025-03-01 10:00:00'),
(46, 'S-143', 'Calc-202', 3, 20.0, 1980, '2025-03-01 10:00:00'),
(47, 'S-144', 'Algebra-101', 1, 80.0, 720, '2025-03-01 10:00:00'),
(48, 'S-145', 'Algebra-101', 1, 65.0, 1120, '2025-03-01 10:00:00'),
(49, 'S-146', 'Algebra-101', 1, 28.0, 1650, '2025-03-01 10:00:00'),
(50, 'S-147', 'Algebra-101', 1, 97.0, 270, '2025-03-01 10:00:00');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Learning Velocity
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EdTechDB.Learning.Silver.StudentPerformance AS
SELECT 
    StudentID,
    ModuleID,
    AVG(ScorePercent) AS AvgScore,
    AVG(TimeTakenSeconds) AS AvgTime,
    MAX(DifficultyTier) AS CurrentTier,
    -- Performance Band
    CASE 
        WHEN AVG(ScorePercent) >= 90 AND AVG(TimeTakenSeconds) < 600 THEN 'High Performer'
        WHEN AVG(ScorePercent) < 60 THEN 'At Risk'
        ELSE 'On Track'
    END AS PerformanceProfile
FROM EdTechDB.Learning.Bronze.QuizAttempts
GROUP BY StudentID, ModuleID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Curriculum Adaptation
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EdTechDB.Learning.Gold.AdaptiveRecommendations AS
SELECT 
    StudentID,
    ModuleID,
    CurrentTier,
    PerformanceProfile,
    -- Recommendation Logic
    CASE 
        WHEN PerformanceProfile = 'High Performer' THEN 'Advance to Tier ' || CAST((CurrentTier + 1) AS VARCHAR)
        WHEN PerformanceProfile = 'At Risk' THEN 'Remedial Content / Tier ' || CAST((CASE WHEN CurrentTier > 1 THEN CurrentTier - 1 ELSE 1 END) AS VARCHAR)
        ELSE 'Maintain Current Path'
    END AS NextAction
FROM EdTechDB.Learning.Silver.StudentPerformance;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all students recommended to 'Advance' to the next tier in the Gold recommendations."

PROMPT 2:
"Calculate the average time taken for 'High Performer' students in 'Algebra-101'."

PROMPT 3:
"Identify how many students are 'At Risk' in 'Calc-202'."
*/
