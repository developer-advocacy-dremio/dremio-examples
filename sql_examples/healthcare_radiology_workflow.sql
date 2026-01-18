/*
    Dremio High-Volume SQL Pattern: Healthcare Radiology Workflow Efficiency
    
    Business Scenario:
    Diagnostic departments need to monitor "Turnaround Time" (TAT) from when a focused scan is completed
    to when the final report is signed off by a Radiologist. Delays impact ED flow and discharge times.
    
    Data Story:
    We track PACS logs (Completed Scans) and Report Sign-off logs. We calculate TAT and flag SLA breaches.
    
    Medallion Architecture:
    - Bronze: PACS_Logs (Scan Times) and Radiol_Reports (Sign-off Times).
      *Volume*: 50+ records typical of a shift.
    - Silver: TAT Calculation (Minutes between Scan and Sign-off).
    - Gold: SLA Breach Report (Exams > 4 hours, or STAT exams > 1 hour).
    
    Key Dremio Features:
    - TIMESTAMPDIFF(MINUTE, ...)
    - JOIN on AccessionID
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareRadiologyDB;
CREATE FOLDER IF NOT EXISTS HealthcareRadiologyDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareRadiologyDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareRadiologyDB.Gold;
USE HealthcareRadiologyDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareRadiologyDB.Bronze.PACS_Logs (
    AccessionID STRING,
    PatientID STRING,
    ExamCode STRING,
    Priority STRING, -- ROUTINE, STAT
    ScanCompleteTime TIMESTAMP,
    Modality STRING -- CT, MRI, XRAY
);

INSERT INTO HealthcareRadiologyDB.Bronze.PACS_Logs VALUES
('ACC100', 'P001', 'CT_HEAD', 'STAT', TIMESTAMP '2025-01-18 08:00:00', 'CT'),
('ACC101', 'P002', 'CXR', 'ROUTINE', TIMESTAMP '2025-01-18 08:15:00', 'XRAY'),
('ACC102', 'P003', 'MRI_BRAIN', 'ROUTINE', TIMESTAMP '2025-01-18 08:30:00', 'MRI'),
('ACC103', 'P004', 'CT_ABD', 'STAT', TIMESTAMP '2025-01-18 09:00:00', 'CT'),
('ACC104', 'P005', 'US_LEG', 'ROUTINE', TIMESTAMP '2025-01-18 09:10:00', 'US'),
-- Batch data
('ACC105', 'P006', 'CT_HEAD', 'STAT', TIMESTAMP '2025-01-18 09:30:00', 'CT'),
('ACC106', 'P007', 'CXR', 'ROUTINE', TIMESTAMP '2025-01-18 09:45:00', 'XRAY'),
('ACC107', 'P008', 'CXR', 'ROUTINE', TIMESTAMP '2025-01-18 10:00:00', 'XRAY'),
('ACC108', 'P009', 'CT_CHEST', 'STAT', TIMESTAMP '2025-01-18 10:15:00', 'CT'),
('ACC109', 'P010', 'MRI_KNEE', 'ROUTINE', TIMESTAMP '2025-01-18 10:30:00', 'MRI'),
('ACC110', 'P011', 'US_ABD', 'ROUTINE', TIMESTAMP '2025-01-18 10:45:00', 'US'),
('ACC111', 'P012', 'CT_HEAD', 'STAT', TIMESTAMP '2025-01-18 11:00:00', 'CT'),
('ACC112', 'P013', 'XRAY_HAND', 'ROUTINE', TIMESTAMP '2025-01-18 11:15:00', 'XRAY'),
('ACC113', 'P014', 'CT_ABD', 'STAT', TIMESTAMP '2025-01-18 11:30:00', 'CT'),
('ACC114', 'P015', 'MRI_SPINE', 'ROUTINE', TIMESTAMP '2025-01-18 12:00:00', 'MRI'),
('ACC115', 'P016', 'XRAY_FOOT', 'ROUTINE', TIMESTAMP '2025-01-18 12:15:00', 'XRAY'),
('ACC116', 'P017', 'CT_PE', 'STAT', TIMESTAMP '2025-01-18 12:30:00', 'CT'),
('ACC117', 'P018', 'US_NECK', 'ROUTINE', TIMESTAMP '2025-01-18 12:45:00', 'US'),
('ACC118', 'P019', 'CT_HEAD', 'STAT', TIMESTAMP '2025-01-18 13:00:00', 'CT'),
('ACC119', 'P020', 'CXR', 'ROUTINE', TIMESTAMP '2025-01-18 13:15:00', 'XRAY'),
('ACC120', 'P021', 'MRI_BRAIN', 'ROUTINE', TIMESTAMP '2025-01-18 13:30:00', 'MRI'),
('ACC121', 'P022', 'CT_ABD', 'STAT', TIMESTAMP '2025-01-18 14:00:00', 'CT'),
('ACC122', 'P023', 'XRAY_CHEST', 'ROUTINE', TIMESTAMP '2025-01-18 14:15:00', 'XRAY'),
('ACC123', 'P024', 'US_RENAL', 'ROUTINE', TIMESTAMP '2025-01-18 14:30:00', 'US'),
('ACC124', 'P025', 'CT_NECK', 'STAT', TIMESTAMP '2025-01-18 14:45:00', 'CT'),
('ACC125', 'P026', 'MRI_HIP', 'ROUTINE', TIMESTAMP '2025-01-18 15:00:00', 'MRI'),
('ACC126', 'P027', 'XRAY_KNEE', 'ROUTINE', TIMESTAMP '2025-01-18 15:15:00', 'XRAY'),
('ACC127', 'P028', 'CT_HEAD', 'STAT', TIMESTAMP '2025-01-18 15:30:00', 'CT'),
('ACC128', 'P029', 'US_DVT', 'ROUTINE', TIMESTAMP '2025-01-18 15:45:00', 'US'),
('ACC129', 'P030', 'MRI_SHOULDER', 'ROUTINE', TIMESTAMP '2025-01-18 16:00:00', 'MRI'),
('ACC130', 'P031', 'CT_CHEST', 'STAT', TIMESTAMP '2025-01-18 16:15:00', 'CT'),
('ACC131', 'P032', 'XRAY_PELVIS', 'ROUTINE', TIMESTAMP '2025-01-18 16:30:00', 'XRAY'),
('ACC132', 'P033', 'US_ABD', 'ROUTINE', TIMESTAMP '2025-01-18 16:45:00', 'US'),
('ACC133', 'P034', 'CT_LSPINE', 'STAT', TIMESTAMP '2025-01-18 17:00:00', 'CT'),
('ACC134', 'P035', 'MRI_BRAIN', 'ROUTINE', TIMESTAMP '2025-01-18 17:15:00', 'MRI'),
('ACC135', 'P036', 'XRAY_TIBIA', 'ROUTINE', TIMESTAMP '2025-01-18 17:30:00', 'XRAY'),
('ACC136', 'P037', 'CT_HEAD', 'STAT', TIMESTAMP '2025-01-18 17:45:00', 'CT'),
('ACC137', 'P038', 'US_THYROID', 'ROUTINE', TIMESTAMP '2025-01-18 18:00:00', 'US'),
('ACC138', 'P039', 'MRI_ANKLE', 'ROUTINE', TIMESTAMP '2025-01-18 18:15:00', 'MRI'),
('ACC139', 'P040', 'CT_ABD', 'STAT', TIMESTAMP '2025-01-18 18:30:00', 'CT'),
('ACC140', 'P041', 'XRAY_SPINE', 'ROUTINE', TIMESTAMP '2025-01-18 18:45:00', 'XRAY'),
('ACC141', 'P042', 'US_ABDOMEN', 'ROUTINE', TIMESTAMP '2025-01-18 19:00:00', 'US'),
('ACC142', 'P043', 'CT_HEAD', 'STAT', TIMESTAMP '2025-01-18 19:15:00', 'CT'),
('ACC143', 'P044', 'MRI_BRAIN', 'ROUTINE', TIMESTAMP '2025-01-18 19:30:00', 'MRI'),
('ACC144', 'P045', 'XRAY_FINGER', 'ROUTINE', TIMESTAMP '2025-01-18 19:45:00', 'XRAY'),
('ACC145', 'P046', 'CT_CHEST', 'STAT', TIMESTAMP '2025-01-18 20:00:00', 'CT'),
('ACC146', 'P047', 'US_PELVIC', 'ROUTINE', TIMESTAMP '2025-01-18 20:15:00', 'US'),
('ACC147', 'P048', 'MRI_KNEE', 'ROUTINE', TIMESTAMP '2025-01-18 20:30:00', 'MRI'),
('ACC148', 'P049', 'CT_ANGIO', 'STAT', TIMESTAMP '2025-01-18 20:45:00', 'CT'),
('ACC149', 'P050', 'XRAY_RIB', 'ROUTINE', TIMESTAMP '2025-01-18 21:00:00', 'XRAY');

CREATE OR REPLACE TABLE HealthcareRadiologyDB.Bronze.Radiol_Reports (
    ReportID STRING,
    AccessionID STRING,
    RadiologistID STRING,
    SignOffTime TIMESTAMP
);

INSERT INTO HealthcareRadiologyDB.Bronze.Radiol_Reports VALUES
('R001', 'ACC100', 'RAD01', TIMESTAMP '2025-01-18 08:45:00'), -- 45m (Good for STAT)
('R002', 'ACC101', 'RAD02', TIMESTAMP '2025-01-18 10:15:00'), -- 2h (Good for Routine)
('R003', 'ACC102', 'RAD01', TIMESTAMP '2025-01-18 09:30:00'), -- 1h
('R004', 'ACC103', 'RAD03', TIMESTAMP '2025-01-18 11:30:00'), -- 2.5h (BAD for STAT)
('R005', 'ACC104', 'RAD02', TIMESTAMP '2025-01-18 10:00:00'), -- 50m
('R006', 'ACC105', 'RAD01', TIMESTAMP '2025-01-18 10:00:00'), -- 30m
('R007', 'ACC106', 'RAD02', TIMESTAMP '2025-01-18 14:00:00'), -- 4h 15m (Late Routine)
('R008', 'ACC107', 'RAD03', TIMESTAMP '2025-01-18 10:45:00'), -- 45m
('R009', 'ACC108', 'RAD01', TIMESTAMP '2025-01-18 10:30:00'), -- 15m
('R010', 'ACC109', 'RAD02', TIMESTAMP '2025-01-18 11:30:00'), -- 1h
-- Bulk Reports
('R011', 'ACC110', 'RAD03', TIMESTAMP '2025-01-18 11:45:00'),
('R012', 'ACC111', 'RAD01', TIMESTAMP '2025-01-18 11:20:00'),
('R013', 'ACC112', 'RAD02', TIMESTAMP '2025-01-18 13:15:00'),
('R014', 'ACC113', 'RAD03', TIMESTAMP '2025-01-18 13:00:00'), -- 1.5h STAT (Breach)
('R015', 'ACC114', 'RAD01', TIMESTAMP '2025-01-18 13:00:00'),
('R016', 'ACC115', 'RAD02', TIMESTAMP '2025-01-18 15:00:00'), -- 2h 45m
('R017', 'ACC116', 'RAD03', TIMESTAMP '2025-01-18 12:40:00'), -- 10m
('R018', 'ACC117', 'RAD01', TIMESTAMP '2025-01-18 13:30:00'),
('R019', 'ACC118', 'RAD02', TIMESTAMP '2025-01-18 13:20:00'),
('R020', 'ACC119', 'RAD03', TIMESTAMP '2025-01-18 15:00:00'),
('R021', 'ACC120', 'RAD01', TIMESTAMP '2025-01-18 14:30:00'),
('R022', 'ACC121', 'RAD02', TIMESTAMP '2025-01-18 14:15:00'),
('R023', 'ACC122', 'RAD03', TIMESTAMP '2025-01-18 15:00:00'),
('R024', 'ACC123', 'RAD01', TIMESTAMP '2025-01-18 15:30:00'),
('R025', 'ACC124', 'RAD02', TIMESTAMP '2025-01-18 15:00:00'),
('R026', 'ACC125', 'RAD03', TIMESTAMP '2025-01-18 16:30:00'),
('R027', 'ACC126', 'RAD01', TIMESTAMP '2025-01-18 16:00:00'),
('R028', 'ACC127', 'RAD02', TIMESTAMP '2025-01-18 15:45:00'), -- 15m
('R029', 'ACC128', 'RAD03', TIMESTAMP '2025-01-18 16:30:00'),
('R030', 'ACC129', 'RAD01', TIMESTAMP '2025-01-18 17:00:00'),
('R031', 'ACC130', 'RAD02', TIMESTAMP '2025-01-18 16:25:00'),
('R032', 'ACC131', 'RAD03', TIMESTAMP '2025-01-18 17:30:00'),
('R033', 'ACC132', 'RAD01', TIMESTAMP '2025-01-18 17:15:00'),
('R034', 'ACC133', 'RAD02', TIMESTAMP '2025-01-18 17:20:00'),
('R035', 'ACC134', 'RAD03', TIMESTAMP '2025-01-18 18:00:00'),
('R036', 'ACC135', 'RAD01', TIMESTAMP '2025-01-18 18:15:00'),
('R037', 'ACC136', 'RAD02', TIMESTAMP '2025-01-18 18:00:00'),
('R038', 'ACC137', 'RAD03', TIMESTAMP '2025-01-18 18:30:00'),
('R039', 'ACC138', 'RAD01', TIMESTAMP '2025-01-18 19:15:00'),
('R040', 'ACC139', 'RAD02', TIMESTAMP '2025-01-18 18:45:00'),
('R041', 'ACC140', 'RAD03', TIMESTAMP '2025-01-18 19:30:00'),
('R042', 'ACC141', 'RAD01', TIMESTAMP '2025-01-18 19:45:00'),
('R043', 'ACC142', 'RAD02', TIMESTAMP '2025-01-18 19:25:00'),
('R044', 'ACC143', 'RAD03', TIMESTAMP '2025-01-18 20:30:00'),
('R045', 'ACC144', 'RAD01', TIMESTAMP '2025-01-18 21:00:00'),
('R046', 'ACC145', 'RAD02', TIMESTAMP '2025-01-18 20:20:00'),
('R047', 'ACC146', 'RAD03', TIMESTAMP '2025-01-18 20:45:00'),
('R048', 'ACC147', 'RAD01', TIMESTAMP '2025-01-18 21:30:00'),
('R049', 'ACC148', 'RAD02', TIMESTAMP '2025-01-18 21:00:00'),
('R050', 'ACC149', 'RAD03', TIMESTAMP '2025-01-18 22:30:00'); -- 1.5h Routine

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Turnaround Time (TAT) Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareRadiologyDB.Silver.TurnaroundTimes AS
SELECT
    p.AccessionID,
    p.PatientID,
    p.ExamCode,
    p.Priority,
    p.Modality,
    p.ScanCompleteTime,
    r.SignOffTime,
    r.RadiologistID,
    TIMESTAMPDIFF(MINUTE, p.ScanCompleteTime, r.SignOffTime) AS TatMinutes
FROM HealthcareRadiologyDB.Bronze.PACS_Logs p
LEFT JOIN HealthcareRadiologyDB.Bronze.Radiol_Reports r ON p.AccessionID = r.AccessionID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: SLA Breach Report
-------------------------------------------------------------------------------
-- SLAs:
-- STAT: 60 mins
-- ROUTINE: 240 mins (4 hours)

CREATE OR REPLACE VIEW HealthcareRadiologyDB.Gold.SLABreachReport AS
SELECT
    AccessionID,
    ExamCode,
    Priority,
    RadiologistID,
    TatMinutes,
    CASE
        WHEN Priority = 'STAT' AND TatMinutes > 60 THEN 'BREACH'
        WHEN Priority = 'ROUTINE' AND TatMinutes > 240 THEN 'BREACH'
        ELSE 'MET'
    END AS SLA_Status
FROM HealthcareRadiologyDB.Silver.TurnaroundTimes
WHERE SignOffTime IS NOT NULL;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me the percentage of STAT exams that breached the 60 min SLA."
    2. "Calculate the average turnaround time by RadiologistID."
    3. "Which Modality has the longest average turnaround time?"
    4. "List all SLA breaches from the last 24 hours."
    5. "Compare TAT for CT vs MRI."
*/
