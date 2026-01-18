/*
    Dremio High-Volume SQL Pattern: Healthcare Patient No-Show Prediction
    
    Business Scenario:
    Clinics lose revenue when patients don't show up. Identifying high-risk appointments allows
    schedulers to double-book or call proactively.
    
    Data Story:
    We look at historical appointments and patient demographics.
    
    Medallion Architecture:
    - Bronze: AppointmentHistory, Patients.
      *Volume*: 50+ records.
    - Silver: NoShowFeatures (Calculating historical no-show rates per patient).
    - Gold: RiskPrediction (Flagging future appointments based on history).
    
    Key Dremio Features:
    - Window Functions (AVG over Partition)
    - CASE Logic
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareClinicDB;
CREATE FOLDER IF NOT EXISTS HealthcareClinicDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareClinicDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareClinicDB.Gold;
USE HealthcareClinicDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareClinicDB.Bronze.AppointmentHistory (
    ApptID STRING,
    PatientID STRING,
    ApptDate DATE,
    ApptType STRING, -- FollowUp, New, Annual
    Status STRING -- Completed, NoShow, Cancelled
);

-- Insert historical data (Mix of behaviors)
INSERT INTO HealthcareClinicDB.Bronze.AppointmentHistory VALUES
('A001', 'P001', DATE '2024-01-01', 'New', 'Completed'),
('A002', 'P001', DATE '2024-02-01', 'FollowUp', 'Completed'),
('A003', 'P002', DATE '2024-01-01', 'New', 'NoShow'),
('A004', 'P002', DATE '2024-02-01', 'New', 'NoShow'), -- Chronic No-Show
('A005', 'P003', DATE '2024-01-10', 'Annual', 'Completed');
-- Bulk Appointment History
INSERT INTO HealthcareClinicDB.Bronze.AppointmentHistory
SELECT 
  'A' || CAST(rn + 100 AS STRING),
  'P' || CAST((rn % 20) + 100 AS STRING), -- 20 recurring patients
  DATE_SUB(DATE '2025-01-01', CAST(rn AS INT)),
  CASE WHEN rn % 3 = 0 THEN 'New' ELSE 'FollowUp' END,
  CASE WHEN (rn % 20) + 100 IN (102, 105, 110) THEN 'NoShow' ELSE 'Completed' END -- Specific patients likely to no-show
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareClinicDB.Bronze.FutureAppointments (
    ApptID STRING,
    PatientID STRING,
    ApptDate DATE,
    TimeSlot STRING
);

INSERT INTO HealthcareClinicDB.Bronze.FutureAppointments VALUES
('FA001', 'P002', DATE '2025-02-01', '09:00'), -- High risk
('FA002', 'P001', DATE '2025-02-01', '10:00'), -- Low risk
('FA003', 'P102', DATE '2025-02-02', '11:00'); -- High risk from bulk gen

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Feature Engineering
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareClinicDB.Silver.PatientRiskProfiles AS
SELECT
    PatientID,
    COUNT(ApptID) AS TotalPastAppts,
    SUM(CASE WHEN Status = 'NoShow' THEN 1 ELSE 0 END) AS NoShowCount,
    (CAST(SUM(CASE WHEN Status = 'NoShow' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(ApptID)) AS NoShowRate
FROM HealthcareClinicDB.Bronze.AppointmentHistory
GROUP BY PatientID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Risk Prediction List
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareClinicDB.Gold.FutureRiskList AS
SELECT
    fa.ApptID,
    fa.PatientID,
    fa.ApptDate,
    fa.TimeSlot,
    COALESCE(rp.NoShowRate, 0.0) AS HistoricalNoShowRate,
    CASE
        WHEN rp.NoShowRate >= 0.5 THEN 'HIGH RISK'
        WHEN rp.NoShowRate >= 0.2 THEN 'MODERATE RISK'
        ELSE 'LOW RISK'
    END AS Prediction
FROM HealthcareClinicDB.Bronze.FutureAppointments fa
LEFT JOIN HealthcareClinicDB.Silver.PatientRiskProfiles rp ON fa.PatientID = rp.PatientID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all HIGH RISK appointments for next week."
    2. "Show the average no-show rate for the clinic history."
    3. "Which patients have a 100% no-show rate?"
    4. "Count future appointments by Risk Category."
*/
