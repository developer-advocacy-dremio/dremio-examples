/*
    Dremio High-Volume SQL Pattern: Healthcare Sepsis Early Warning
    
    Business Scenario:
    Hospitals need to detect Sepsis early by monitoring real-time vitals and lab results against
    SIRS (Systemic Inflammatory Response Syndrome) criteria. 
    Time is critical; finding patients who meet 2+ SIRS criteria allows for rapid intervention.
    
    Data Story:
    We ingest high-frequency vitals (Temp, HR, Respiratory Rate) and Lab results (WBC).
    
    Medallion Architecture:
    - Bronze: Raw streams of Vitals (VitalsStream) and Lab results (LabResults).
      *Volume*: 50+ records simulating a busy ward.
    - Silver: Normalized events checking against SIRS thresholds.
    - Gold: Patient-level dashboard flagging those meeting >2 criteria.
    
    Key Dremio Features:
    - CASE WHEN Logic (Complex Clinical Rules)
    - Aggregation (COUNT_IF)
    - Timestamps
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareSepsisDB;
CREATE FOLDER IF NOT EXISTS HealthcareSepsisDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareSepsisDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareSepsisDB.Gold;
USE HealthcareSepsisDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareSepsisDB.Bronze.Admissions (
    VisitID STRING,
    PatientID STRING,
    AdmitTime TIMESTAMP,
    Unit STRING
);

INSERT INTO HealthcareSepsisDB.Bronze.Admissions VALUES
('V100', 'P001', TIMESTAMP '2025-01-18 08:00:00', 'ICU'),
('V101', 'P002', TIMESTAMP '2025-01-18 09:15:00', 'ED'),
('V102', 'P003', TIMESTAMP '2025-01-18 10:30:00', 'MED-SURG'),
('V103', 'P004', TIMESTAMP '2025-01-18 11:00:00', 'ICU'),
('V104', 'P005', TIMESTAMP '2025-01-18 11:45:00', 'ED');

CREATE OR REPLACE TABLE HealthcareSepsisDB.Bronze.VitalsStream (
    MeasurementID STRING,
    VisitID STRING,
    RecordedTime TIMESTAMP,
    Temp_C DOUBLE,
    HeartRate INT,
    RespRate INT
);

-- Insert 50+ records of vitals
INSERT INTO HealthcareSepsisDB.Bronze.VitalsStream VALUES
-- P001: Stable
('M001', 'V100', TIMESTAMP '2025-01-18 08:30:00', 37.0, 75, 14),
('M002', 'V100', TIMESTAMP '2025-01-18 09:30:00', 37.1, 78, 15),
('M003', 'V100', TIMESTAMP '2025-01-18 10:30:00', 37.0, 76, 14),
-- P002: Deteriorating (High HR, High Temp)
('M004', 'V101', TIMESTAMP '2025-01-18 09:30:00', 38.5, 95, 22), -- SIRS: Temp>38, HR>90, RR>20
('M005', 'V101', TIMESTAMP '2025-01-18 10:30:00', 39.1, 110, 24),
('M006', 'V101', TIMESTAMP '2025-01-18 11:30:00', 39.5, 120, 28),
-- P003: Hypothermia risk (SIRS can be <36C)
('M007', 'V102', TIMESTAMP '2025-01-18 11:00:00', 35.5, 80, 16),
('M008', 'V102', TIMESTAMP '2025-01-18 12:00:00', 35.1, 85, 18),
-- P004-P020: Background noise
('M009', 'V103', TIMESTAMP '2025-01-18 11:15:00', 37.0, 70, 12),
('M010', 'V104', TIMESTAMP '2025-01-18 12:00:00', 37.2, 72, 14),
('M011', 'V100', TIMESTAMP '2025-01-18 11:30:00', 37.0, 75, 14),
('M012', 'V100', TIMESTAMP '2025-01-18 12:30:00', 37.0, 74, 14),
('M013', 'V101', TIMESTAMP '2025-01-18 12:30:00', 39.8, 130, 30),
('M014', 'V103', TIMESTAMP '2025-01-18 12:15:00', 36.8, 72, 12),
('M015', 'V103', TIMESTAMP '2025-01-18 13:15:00', 36.9, 74, 13),
('M016', 'V104', TIMESTAMP '2025-01-18 13:00:00', 37.5, 80, 16),
('M017', 'V104', TIMESTAMP '2025-01-18 14:00:00', 37.6, 82, 16),
('M018', 'V100', TIMESTAMP '2025-01-18 13:30:00', 37.1, 75, 14),
('M019', 'V100', TIMESTAMP '2025-01-18 14:30:00', 37.0, 76, 14),
('M020', 'V101', TIMESTAMP '2025-01-18 13:30:00', 40.0, 140, 35), -- Critical
('M021', 'V102', TIMESTAMP '2025-01-18 13:00:00', 36.0, 80, 16), -- Stabilizing
('M022', 'V103', TIMESTAMP '2025-01-18 14:15:00', 37.0, 70, 12),
('M023', 'V104', TIMESTAMP '2025-01-18 15:00:00', 37.2, 72, 14),
('M024', 'V100', TIMESTAMP '2025-01-18 15:30:00', 37.0, 75, 14),
('M025', 'V100', TIMESTAMP '2025-01-18 16:30:00', 37.0, 74, 14),
('M026', 'V101', TIMESTAMP '2025-01-18 14:30:00', 39.5, 135, 32),
('M027', 'V102', TIMESTAMP '2025-01-18 14:00:00', 36.5, 82, 16),
('M028', 'V103', TIMESTAMP '2025-01-18 15:15:00', 36.8, 71, 12),
('M029', 'V104', TIMESTAMP '2025-01-18 16:00:00', 37.5, 81, 16),
('M030', 'V104', TIMESTAMP '2025-01-18 17:00:00', 37.6, 83, 16),
('M031', 'V100', TIMESTAMP '2025-01-18 17:30:00', 37.1, 75, 14),
('M032', 'V100', TIMESTAMP '2025-01-18 18:30:00', 37.0, 76, 14),
('M033', 'V101', TIMESTAMP '2025-01-18 15:30:00', 39.0, 125, 28), -- Intervention likely happened
('M034', 'V102', TIMESTAMP '2025-01-18 15:00:00', 36.8, 80, 16),
('M035', 'V103', TIMESTAMP '2025-01-18 16:15:00', 37.0, 70, 12),
('M036', 'V104', TIMESTAMP '2025-01-18 18:00:00', 37.2, 72, 14),
('M037', 'V100', TIMESTAMP '2025-01-18 19:30:00', 37.0, 75, 14),
('M038', 'V100', TIMESTAMP '2025-01-18 20:30:00', 37.0, 74, 14),
('M039', 'V101', TIMESTAMP '2025-01-18 16:30:00', 38.5, 110, 24),
('M040', 'V102', TIMESTAMP '2025-01-18 16:00:00', 37.0, 78, 16),
('M041', 'V103', TIMESTAMP '2025-01-18 17:15:00', 36.8, 72, 12),
('M042', 'V104', TIMESTAMP '2025-01-18 19:00:00', 37.5, 80, 16),
('M043', 'V104', TIMESTAMP '2025-01-18 20:00:00', 37.6, 82, 16),
('M044', 'V100', TIMESTAMP '2025-01-18 21:30:00', 37.1, 75, 14),
('M045', 'V100', TIMESTAMP '2025-01-18 22:30:00', 37.0, 76, 14),
('M046', 'V101', TIMESTAMP '2025-01-18 17:30:00', 38.0, 100, 22),
('M047', 'V102', TIMESTAMP '2025-01-18 17:00:00', 37.0, 75, 16),
('M048', 'V103', TIMESTAMP '2025-01-18 18:15:00', 37.0, 70, 12),
('M049', 'V104', TIMESTAMP '2025-01-18 21:00:00', 37.2, 72, 14),
('M050', 'V105', TIMESTAMP '2025-01-18 09:00:00', 39.0, 95, 22); -- New patient, septic

CREATE OR REPLACE TABLE HealthcareSepsisDB.Bronze.LabResults (
    LabID STRING,
    VisitID STRING,
    ResultTime TIMESTAMP,
    WBC_Count DOUBLE -- White Blood Cell Count (10^9/L)
);

INSERT INTO HealthcareSepsisDB.Bronze.LabResults VALUES
('L001', 'V100', TIMESTAMP '2025-01-18 08:45:00', 6.0),  -- Normal (4-11)
('L002', 'V101', TIMESTAMP '2025-01-18 09:45:00', 14.5), -- High (>12 is SIRS)
('L003', 'V102', TIMESTAMP '2025-01-18 11:15:00', 3.0),  -- Low (<4 is SIRS)
('L004', 'V103', TIMESTAMP '2025-01-18 11:30:00', 7.5),
('L005', 'V105', TIMESTAMP '2025-01-18 09:30:00', 13.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: SIRS Scoring
-------------------------------------------------------------------------------
-- SIRS Criteria:
-- 1. Temp > 38C OR < 36C
-- 2. HR > 90
-- 3. RR > 20
-- 4. WBC > 12 OR < 4 (We will join nearest lab result)

CREATE OR REPLACE VIEW HealthcareSepsisDB.Silver.SIRS_Events AS
SELECT
    v.VisitID,
    v.RecordedTime,
    v.Temp_C,
    v.HeartRate,
    v.RespRate,
    l.WBC_Count,
    (CASE WHEN v.Temp_C > 38.0 OR v.Temp_C < 36.0 THEN 1 ELSE 0 END) AS Score_Temp,
    (CASE WHEN v.HeartRate > 90 THEN 1 ELSE 0 END) AS Score_HR,
    (CASE WHEN v.RespRate > 20 THEN 1 ELSE 0 END) AS Score_RR,
    (CASE WHEN l.WBC_Count > 12.0 OR l.WBC_Count < 4.0 THEN 1 ELSE 0 END) AS Score_WBC
FROM HealthcareSepsisDB.Bronze.VitalsStream v
LEFT JOIN HealthcareSepsisDB.Bronze.LabResults l ON v.VisitID = l.VisitID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Sepsis Alert Dashboard
-------------------------------------------------------------------------------
-- Alert if Total Score >= 2
CREATE OR REPLACE VIEW HealthcareSepsisDB.Gold.SepsisAlerts AS
SELECT
    VisitID,
    RecordedTime,
    (Score_Temp + Score_HR + Score_RR + COALESCE(Score_WBC, 0)) AS TotalSIRS_Score,
    CASE 
        WHEN (Score_Temp + Score_HR + Score_RR + COALESCE(Score_WBC, 0)) >= 2 THEN 'SEPSIS ALERT'
        ELSE 'Normal'
    END AS AlertStatus,
    Temp_C,
    HeartRate,
    RespRate,
    WBC_Count
FROM HealthcareSepsisDB.Silver.SIRS_Events
WHERE (Score_Temp + Score_HR + Score_RR + COALESCE(Score_WBC, 0)) >= 2;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me all active Sepsis Alerts ordered by time."
    2. "Count the number of alerts by Patient/VisitID."
    3. "What is the average Heart Rate for patients with a Sepsis Alert?"
    4. "Show the trend of SIRS scores over time for VisitID V101."
    5. "List patients who met >3 SIRS criteria (Severe risk)."
*/
