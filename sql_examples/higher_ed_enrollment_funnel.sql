/*
 * Higher Education Enrollment Funnel Demo
 * 
 * Scenario:
 * Analyzing the student admissions pipeline from Application to Matriculation.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.HigherEd;
CREATE FOLDER IF NOT EXISTS RetailDB.HigherEd.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.HigherEd.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.HigherEd.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.HigherEd.Bronze.Applicants (
    ApplicantID INT,
    Term VARCHAR, -- Fall 2025
    MajorOfInterest VARCHAR,
    GPA DOUBLE,
    Status VARCHAR, -- Applied, Admitted, Waitlisted, Enrolled, Denied
    DepositPaid BOOLEAN
);

INSERT INTO RetailDB.HigherEd.Bronze.Applicants VALUES
(1, 'Fall 2025', 'Computer Science', 3.8, 'Enrolled', true),
(2, 'Fall 2025', 'Engineering', 3.5, 'Admitted', false), -- Melt
(3, 'Fall 2025', 'Business', 3.2, 'Denied', false),
(4, 'Fall 2025', 'Computer Science', 3.9, 'Enrolled', true),
(5, 'Fall 2025', 'Biology', 3.6, 'Waitlisted', false),
(6, 'Fall 2025', 'Business', 3.4, 'Admitted', true), -- Enrolled status pending?
(7, 'Fall 2025', 'Engineering', 3.7, 'Enrolled', true),
(8, 'Fall 2025', 'Psychology', 3.1, 'Denied', false),
(9, 'Fall 2025', 'Computer Science', 4.0, 'Admitted', false),
(10, 'Fall 2025', 'Business', 3.5, 'Enrolled', true),
(11, 'Fall 2025', 'Biology', 3.8, 'Waitlisted', false);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.HigherEd.Silver.FunnelMetrics AS
SELECT 
    Term,
    MajorOfInterest,
    Status,
    DepositPaid,
    CASE 
        WHEN Status = 'Enrolled' OR (Status = 'Admitted' AND DepositPaid = true) THEN 'Matriculated'
        ELSE Status 
    END AS FinalStage
FROM RetailDB.HigherEd.Bronze.Applicants;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.HigherEd.Gold.MajorYield AS
SELECT 
    MajorOfInterest,
    COUNT(*) AS TotalApplicants,
    SUM(CASE WHEN Status IN ('Admitted', 'Enrolled') THEN 1 ELSE 0 END) AS AdmittedCount,
    SUM(CASE WHEN FinalStage = 'Matriculated' THEN 1 ELSE 0 END) AS EnrolledCount,
    (CAST(SUM(CASE WHEN FinalStage = 'Matriculated' THEN 1 ELSE 0 END) AS DOUBLE) / 
     NULLIF(SUM(CASE WHEN Status IN ('Admitted', 'Enrolled') THEN 1 ELSE 0 END), 0)) * 100 AS YieldRate
FROM RetailDB.HigherEd.Silver.FunnelMetrics
GROUP BY MajorOfInterest;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Calculate the YieldRate for 'Computer Science' majors in RetailDB.HigherEd.Gold.MajorYield."
*/
