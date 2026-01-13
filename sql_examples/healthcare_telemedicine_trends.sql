/*
 * Telemedicine Utilization Demo
 * 
 * Scenario:
 * Analyzing adoption trends of remote consultations vs in-person visits.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Telehealth;
CREATE FOLDER IF NOT EXISTS RetailDB.Telehealth.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Telehealth.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Telehealth.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Telehealth.Bronze.Visits (
    VisitID INT,
    PatientID INT,
    ProviderSpecialty VARCHAR,
    VisitType VARCHAR, -- Video, Phone, In-Person
    DurationMinutes INT,
    Outcome VARCHAR, -- Prescription, Follow-up, Resolved
    VisitDate DATE
);

INSERT INTO RetailDB.Telehealth.Bronze.Visits VALUES
(1, 101, 'General Practice', 'Video', 15, 'Prescription', '2025-01-01'),
(2, 102, 'Dermatology', 'Video', 20, 'Resolved', '2025-01-01'),
(3, 103, 'General Practice', 'In-Person', 45, 'Follow-up', '2025-01-01'),
(4, 104, 'Psychiatry', 'Video', 60, 'Follow-up', '2025-01-02'),
(5, 105, 'Cardiology', 'In-Person', 30, 'Prescription', '2025-01-02'),
(6, 106, 'General Practice', 'Phone', 10, 'Resolved', '2025-01-02'),
(7, 107, 'Psychiatry', 'Phone', 50, 'Follow-up', '2025-01-03'),
(8, 108, 'Dermatology', 'In-Person', 25, 'Procedure', '2025-01-03'),
(9, 109, 'General Practice', 'Video', 12, 'Referral', '2025-01-04'),
(10, 110, 'Pediatrics', 'In-Person', 40, 'Prescription', '2025-01-04'),
(11, 111, 'Pediatrics', 'Video', 15, 'Resolved', '2025-01-05');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Telehealth.Silver.EnrichedVisits AS
SELECT 
    VisitID,
    ProviderSpecialty,
    VisitType,
    CASE 
        WHEN VisitType IN ('Video', 'Phone') THEN 'Remote'
        ELSE 'In-Person'
    END AS Category,
    DurationMinutes,
    Outcome
FROM RetailDB.Telehealth.Bronze.Visits;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Telehealth.Gold.AdoptionTrends AS
SELECT 
    ProviderSpecialty,
    Category,
    COUNT(*) AS VisitCount,
    AVG(DurationMinutes) AS AvgDuration
FROM RetailDB.Telehealth.Silver.EnrichedVisits
GROUP BY ProviderSpecialty, Category;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which specialty has the highest number of Remote visits in RetailDB.Telehealth.Gold.AdoptionTrends?"
*/
