/*
 * Chronic Disease Management Demo
 * 
 * Scenario:
 * Monitoring patient cohorts (Diabetes, Hypertension) for care gaps and HbA1c control.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.ChronicCare;
CREATE FOLDER IF NOT EXISTS RetailDB.ChronicCare.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.ChronicCare.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.ChronicCare.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.ChronicCare.Bronze.PatientRegistry (
    PatientID INT,
    ConditionGroup VARCHAR, -- Diabetes, Hypertension
    LastVisitDate DATE,
    BiometricValue DOUBLE -- e.g., HbA1c %, BP Systolic
);

INSERT INTO RetailDB.ChronicCare.Bronze.PatientRegistry VALUES
(1, 'Diabetes', '2024-12-01', 6.5), -- Good
(2, 'Diabetes', '2024-11-15', 8.2), -- High
(3, 'Diabetes', '2024-06-01', 7.0), -- Gap in care (> 6 months)
(4, 'Hypertension', '2025-01-01', 120),
(5, 'Hypertension', '2025-01-02', 150), -- High
(6, 'Diabetes', '2025-01-05', 9.5), -- Critical
(7, 'Diabetes', '2024-12-20', 5.9),
(8, 'Hypertension', '2024-05-01', 130), -- Gap
(9, 'Hypertension', '2024-12-10', 125),
(10, 'Diabetes', '2025-01-03', 7.5);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.ChronicCare.Silver.CareGaps AS
SELECT 
    PatientID,
    ConditionGroup,
    BiometricValue,
    LastVisitDate,
    DATE_DIFF(CAST(CURRENT_DATE AS TIMESTAMP), CAST(LastVisitDate AS TIMESTAMP)) AS DaysSinceLastVisit,
    CASE 
        WHEN ConditionGroup = 'Diabetes' AND BiometricValue > 8.0 THEN 'Uncontrolled'
        WHEN ConditionGroup = 'Hypertension' AND BiometricValue > 140 THEN 'Uncontrolled'
        ELSE 'Controlled'
    END AS HealthStatus
FROM RetailDB.ChronicCare.Bronze.PatientRegistry;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.ChronicCare.Gold.PopHealthSummary AS
SELECT 
    ConditionGroup,
    COUNT(*) AS TotalPatients,
    SUM(CASE WHEN HealthStatus = 'Uncontrolled' THEN 1 ELSE 0 END) AS UncontrolledCount,
    SUM(CASE WHEN DaysSinceLastVisit > 180 THEN 1 ELSE 0 END) AS CareGapCount
FROM RetailDB.ChronicCare.Silver.CareGaps
GROUP BY ConditionGroup;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show the number of patients with 'Care Gaps' (visits > 180 days ago) by condition in RetailDB.ChronicCare.Gold.PopHealthSummary."
*/
