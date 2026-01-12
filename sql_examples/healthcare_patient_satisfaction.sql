/*
 * Patient Satisfaction (HCAHPS) Demo
 * 
 * Scenario:
 * Analyzing survey results to improve patient experience scores.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.PatientExp;
CREATE FOLDER IF NOT EXISTS RetailDB.PatientExp.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.PatientExp.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.PatientExp.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.PatientExp.Bronze.Surveys (
    SurveyID INT,
    Department VARCHAR,
    NurseCommScore INT, -- 1 to 10
    DoctorCommScore INT, -- 1 to 10
    CleanlinessScore INT, -- 1 to 10
    Comments VARCHAR
);

INSERT INTO RetailDB.PatientExp.Bronze.Surveys VALUES
(1, 'ER', 8, 9, 7, 'Good service'),
(2, 'ER', 5, 6, 4, 'Long wait'),
(3, 'Cardiology', 10, 10, 10, 'Excellent care'),
(4, 'Cardiology', 9, 8, 9, 'Very professional'),
(5, 'Pediatrics', 10, 10, 10, 'Great with kids'),
(6, 'Pediatrics', 8, 7, 8, 'Good'),
(7, 'ER', 4, 3, 5, 'Rude staff'),
(8, 'General Surgery', 9, 9, 8, 'Successful surgery'),
(9, 'General Surgery', 7, 8, 7, 'Okay'),
(10, 'Oncology', 10, 10, 9, 'Compassionate team'),
(11, 'ER', 6, 7, 6, 'Average'),
(12, 'Cardiology', 9, 9, 8, 'Happy');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.PatientExp.Silver.ScoredSurveys AS
SELECT 
    SurveyID,
    Department,
    (NurseCommScore + DoctorCommScore + CleanlinessScore) / 3.0 AS CompositeScore,
    CASE 
        WHEN NurseCommScore >= 9 AND DoctorCommScore >= 9 THEN 'Promoter'
        WHEN NurseCommScore <= 6 OR DoctorCommScore <= 6 THEN 'Detractor'
        ELSE 'Passive'
    END AS NPS_Category
FROM RetailDB.PatientExp.Bronze.Surveys;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.PatientExp.Gold.DeptRankings AS
SELECT 
    Department,
    AVG(CompositeScore) AS AvgScore,
    COUNT(*) AS SurveyCount,
    SUM(CASE WHEN NPS_Category = 'Promoter' THEN 1 ELSE 0 END) AS Promoters,
    SUM(CASE WHEN NPS_Category = 'Detractor' THEN 1 ELSE 0 END) AS Detractors
FROM RetailDB.PatientExp.Silver.ScoredSurveys
GROUP BY Department;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Rank departments by AvgScore descending using RetailDB.PatientExp.Gold.DeptRankings."
*/
