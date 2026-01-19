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

INSERT INTO HealthcareClinicDB.Bronze.AppointmentHistory VALUES
('A001', 'P001', DATE '2024-01-01', 'New', 'Completed'),
('A002', 'P001', DATE '2024-02-01', 'FollowUp', 'Completed'),
('A003', 'P002', DATE '2024-01-01', 'New', 'NoShow'),
('A004', 'P002', DATE '2024-02-01', 'New', 'NoShow'),
('A005', 'P003', DATE '2024-01-10', 'Annual', 'Completed'),
('A006', 'P004', DATE '2024-01-01', 'FollowUp', 'Completed'),
('A007', 'P005', DATE '2024-01-02', 'New', 'Completed'),
('A008', 'P006', DATE '2024-01-03', 'Annual', 'NoShow'),
('A009', 'P007', DATE '2024-01-04', 'New', 'Completed'),
('A010', 'P008', DATE '2024-01-05', 'FollowUp', 'Cancelled'),
('A011', 'P001', DATE '2024-03-01', 'FollowUp', 'Completed'),
('A012', 'P002', DATE '2024-03-01', 'FollowUp', 'NoShow'),
('A013', 'P003', DATE '2024-02-10', 'FollowUp', 'Completed'),
('A014', 'P004', DATE '2024-02-01', 'New', 'Completed'),
('A015', 'P005', DATE '2024-02-02', 'FollowUp', 'NoShow'),
('A016', 'P006', DATE '2024-02-03', 'New', 'Completed'),
('A017', 'P007', DATE '2024-02-04', 'Annual', 'Completed'),
('A018', 'P008', DATE '2024-02-05', 'New', 'Completed'),
('A019', 'P009', DATE '2024-01-06', 'New', 'Completed'),
('A020', 'P010', DATE '2024-01-07', 'FollowUp', 'NoShow'),
('A021', 'P011', DATE '2024-01-08', 'Annual', 'Completed'),
('A022', 'P012', DATE '2024-01-09', 'New', 'Cancelled'),
('A023', 'P013', DATE '2024-01-10', 'FollowUp', 'Completed'),
('A024', 'P014', DATE '2024-01-11', 'New', 'NoShow'),
('A025', 'P015', DATE '2024-01-12', 'Annual', 'Completed'),
('A026', 'P016', DATE '2024-01-13', 'New', 'Completed'),
('A027', 'P017', DATE '2024-01-14', 'FollowUp', 'NoShow'),
('A028', 'P018', DATE '2024-01-15', 'New', 'Completed'),
('A029', 'P019', DATE '2024-01-16', 'Annual', 'Cancelled'),
('A030', 'P020', DATE '2024-01-17', 'New', 'Completed'),
('A031', 'P009', DATE '2024-02-06', 'FollowUp', 'Completed'),
('A032', 'P010', DATE '2024-02-07', 'New', 'NoShow'),
('A033', 'P011', DATE '2024-02-08', 'FollowUp', 'Completed'),
('A034', 'P012', DATE '2024-02-09', 'New', 'Completed'),
('A035', 'P013', DATE '2024-02-10', 'Annual', 'NoShow'),
('A036', 'P014', DATE '2024-02-11', 'FollowUp', 'NoShow'),
('A037', 'P015', DATE '2024-02-12', 'New', 'Completed'),
('A038', 'P016', DATE '2024-02-13', 'FollowUp', 'Completed'),
('A039', 'P017', DATE '2024-02-14', 'Annual', 'NoShow'),
('A040', 'P018', DATE '2024-02-15', 'New', 'Completed'),
('A041', 'P019', DATE '2024-02-16', 'FollowUp', 'Completed'),
('A042', 'P020', DATE '2024-02-17', 'New', 'NoShow'),
('A043', 'P021', DATE '2024-01-18', 'Annual', 'Completed'),
('A044', 'P022', DATE '2024-01-19', 'New', 'Completed'),
('A045', 'P023', DATE '2024-01-20', 'FollowUp', 'NoShow'),
('A046', 'P024', DATE '2024-01-21', 'New', 'Completed'),
('A047', 'P025', DATE '2024-01-22', 'Annual', 'Cancelled'),
('A048', 'P021', DATE '2024-02-18', 'FollowUp', 'Completed'),
('A049', 'P022', DATE '2024-02-19', 'New', 'Completed'),
('A050', 'P023', DATE '2024-02-20', 'FollowUp', 'NoShow');

CREATE OR REPLACE TABLE HealthcareClinicDB.Bronze.FutureAppointments (
    ApptID STRING,
    PatientID STRING,
    ApptDate DATE,
    TimeSlot STRING
);

INSERT INTO HealthcareClinicDB.Bronze.FutureAppointments VALUES
('FA001', 'P002', DATE '2025-02-01', '09:00'), -- Expect High Risk
('FA002', 'P001', DATE '2025-02-01', '10:00'), -- Low Risk
('FA003', 'P003', DATE '2025-02-02', '11:00'),
('FA004', 'P010', DATE '2025-02-02', '09:30'), -- High Risk
('FA005', 'P017', DATE '2025-02-03', '14:00'), -- High Risk
('FA006', 'P020', DATE '2025-02-03', '10:00'),
('FA007', 'P023', DATE '2025-02-04', '15:30');

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
