/*
    Dremio High-Volume SQL Pattern: Healthcare Pharmacy Safety Loops
    
    Business Scenario:
    Ensuring medication safety involves reconciling what was Dispensed (from Pyxis/ADC)
    vs what was Administered (eMAR scan). Discrepancies can indicate diversion or errors.
    
    Data Story:
    We track Dispense Logs and Administration Logs.
    
    Medallion Architecture:
    - Bronze: DispensingLogs, AdminLogs.
      *Volume*: 50+ records.
    - Silver: AdminVariances (Matched transactions with delta checks).
    - Gold: SafetyIncidentReport (Unmatched or Dose Mismatches).
    
    Key Dremio Features:
    - JOIN checks
    - TIMESTAMPDIFF
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB;
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcarePharmaDB.Gold;
USE HealthcarePharmaDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcarePharmaDB.Bronze.DispensingLogs (
    DispenseID STRING,
    PatientID STRING,
    MedicationID STRING,
    DispenseTime TIMESTAMP,
    User STRING, -- Nurse or Tech
    Quantity INT
);

INSERT INTO HealthcarePharmaDB.Bronze.DispensingLogs VALUES
('D101', 'P101', 'MED01', TIMESTAMP '2025-01-18 08:00:00', 'NURSE_A', 2),
('D102', 'P102', 'MED02', TIMESTAMP '2025-01-18 08:30:00', 'NURSE_B', 1),
('D103', 'P103', 'MED03', TIMESTAMP '2025-01-18 09:00:00', 'NURSE_A', 1),
('D104', 'P104', 'MED01', TIMESTAMP '2025-01-18 09:30:00', 'NURSE_C', 2),
('D105', 'P105', 'MED02', TIMESTAMP '2025-01-18 10:00:00', 'NURSE_B', 1),
('D106', 'P106', 'MED03', TIMESTAMP '2025-01-18 10:30:00', 'NURSE_A', 1),
('D107', 'P107', 'MED01', TIMESTAMP '2025-01-18 11:00:00', 'NURSE_C', 2),
('D108', 'P108', 'MED02', TIMESTAMP '2025-01-18 11:30:00', 'NURSE_B', 1),
('D109', 'P109', 'MED03', TIMESTAMP '2025-01-18 12:00:00', 'NURSE_A', 1),
('D110', 'P110', 'MED01', TIMESTAMP '2025-01-18 12:30:00', 'NURSE_C', 2),
('D111', 'P111', 'MED02', TIMESTAMP '2025-01-18 13:00:00', 'NURSE_B', 1),
('D112', 'P112', 'MED03', TIMESTAMP '2025-01-18 13:30:00', 'NURSE_A', 1),
('D113', 'P113', 'MED01', TIMESTAMP '2025-01-18 14:00:00', 'NURSE_C', 2),
('D114', 'P114', 'MED02', TIMESTAMP '2025-01-18 14:30:00', 'NURSE_B', 1),
('D115', 'P115', 'MED03', TIMESTAMP '2025-01-18 15:00:00', 'NURSE_A', 1),
('D116', 'P116', 'MED01', TIMESTAMP '2025-01-18 15:30:00', 'NURSE_C', 2),
('D117', 'P117', 'MED02', TIMESTAMP '2025-01-18 16:00:00', 'NURSE_B', 1),
('D118', 'P118', 'MED03', TIMESTAMP '2025-01-18 16:30:00', 'NURSE_A', 1),
('D119', 'P119', 'MED01', TIMESTAMP '2025-01-18 17:00:00', 'NURSE_C', 2),
('D120', 'P120', 'MED02', TIMESTAMP '2025-01-18 17:30:00', 'NURSE_B', 1),
('D121', 'P121', 'MED03', TIMESTAMP '2025-01-18 18:00:00', 'NURSE_A', 1),
('D122', 'P122', 'MED01', TIMESTAMP '2025-01-18 18:30:00', 'NURSE_C', 2),
('D123', 'P123', 'MED02', TIMESTAMP '2025-01-18 19:00:00', 'NURSE_B', 1),
('D124', 'P124', 'MED03', TIMESTAMP '2025-01-18 19:30:00', 'NURSE_A', 1),
('D125', 'P125', 'MED01', TIMESTAMP '2025-01-18 20:00:00', 'NURSE_C', 2),
('D126', 'P126', 'MED02', TIMESTAMP '2025-01-18 20:30:00', 'NURSE_B', 1),
('D127', 'P127', 'MED03', TIMESTAMP '2025-01-18 21:00:00', 'NURSE_A', 1),
('D128', 'P128', 'MED01', TIMESTAMP '2025-01-18 21:30:00', 'NURSE_C', 2),
('D129', 'P129', 'MED02', TIMESTAMP '2025-01-18 22:00:00', 'NURSE_B', 1),
('D130', 'P130', 'MED03', TIMESTAMP '2025-01-18 22:30:00', 'NURSE_A', 1),
('D131', 'P131', 'MED01', TIMESTAMP '2025-01-18 23:00:00', 'NURSE_C', 2),
('D132', 'P132', 'MED02', TIMESTAMP '2025-01-18 07:00:00', 'NURSE_B', 1),
('D133', 'P133', 'MED03', TIMESTAMP '2025-01-18 07:30:00', 'NURSE_A', 1),
('D134', 'P134', 'MED01', TIMESTAMP '2025-01-18 06:00:00', 'NURSE_C', 2),
('D135', 'P135', 'MED02', TIMESTAMP '2025-01-18 06:30:00', 'NURSE_B', 1),
('D136', 'P136', 'MED03', TIMESTAMP '2025-01-18 05:00:00', 'NURSE_A', 1),
('D137', 'P137', 'MED01', TIMESTAMP '2025-01-18 05:30:00', 'NURSE_C', 2),
('D138', 'P138', 'MED02', TIMESTAMP '2025-01-18 04:00:00', 'NURSE_B', 1),
('D139', 'P139', 'MED03', TIMESTAMP '2025-01-18 04:30:00', 'NURSE_A', 1),
('D140', 'P140', 'MED01', TIMESTAMP '2025-01-18 03:00:00', 'NURSE_C', 2),
('D141', 'P141', 'MED02', TIMESTAMP '2025-01-18 03:30:00', 'NURSE_B', 1), -- Missing Admin (Diversion)
('D142', 'P142', 'MED03', TIMESTAMP '2025-01-18 02:00:00', 'NURSE_A', 1),
('D143', 'P143', 'MED01', TIMESTAMP '2025-01-18 02:30:00', 'NURSE_C', 2),
('D144', 'P144', 'MED02', TIMESTAMP '2025-01-18 01:00:00', 'NURSE_B', 1),
('D145', 'P145', 'MED03', TIMESTAMP '2025-01-18 01:30:00', 'NURSE_A', 1),
('D146', 'P146', 'MED01', TIMESTAMP '2025-01-18 00:00:00', 'NURSE_C', 2),
('D147', 'P147', 'MED02', TIMESTAMP '2025-01-18 00:30:00', 'NURSE_B', 1),
('D148', 'P148', 'MED03', TIMESTAMP '2025-01-18 23:30:00', 'NURSE_A', 1),
('D149', 'P149', 'MED01', TIMESTAMP '2025-01-19 08:00:00', 'NURSE_C', 2),
('D150', 'P150', 'MED02', TIMESTAMP '2025-01-19 09:00:00', 'NURSE_B', 1);

CREATE OR REPLACE TABLE HealthcarePharmaDB.Bronze.AdminLogs (
    AdminID STRING,
    PatientID STRING,
    MedicationID STRING,
    AdminTime TIMESTAMP,
    User STRING,
    Quantity INT,
    Status STRING -- Given, Held, Refused
);

INSERT INTO HealthcarePharmaDB.Bronze.AdminLogs VALUES
('A101', 'P101', 'MED01', TIMESTAMP '2025-01-18 08:15:00', 'NURSE_A', 2, 'Given'),
('A102', 'P102', 'MED02', TIMESTAMP '2025-01-18 08:45:00', 'NURSE_B', 1, 'Given'),
('A103', 'P103', 'MED03', TIMESTAMP '2025-01-18 09:15:00', 'NURSE_A', 1, 'Given'),
('A104', 'P104', 'MED01', TIMESTAMP '2025-01-18 09:45:00', 'NURSE_C', 2, 'Given'),
('A105', 'P105', 'MED02', TIMESTAMP '2025-01-18 10:15:00', 'NURSE_B', 1, 'Given'),
('A106', 'P106', 'MED03', TIMESTAMP '2025-01-18 10:45:00', 'NURSE_A', 1, 'Given'),
('A107', 'P107', 'MED01', TIMESTAMP '2025-01-18 11:15:00', 'NURSE_C', 2, 'Given'),
('A108', 'P108', 'MED02', TIMESTAMP '2025-01-18 11:45:00', 'NURSE_B', 1, 'Given'),
('A109', 'P109', 'MED03', TIMESTAMP '2025-01-18 12:15:00', 'NURSE_A', 1, 'Given'),
('A110', 'P110', 'MED01', TIMESTAMP '2025-01-18 12:45:00', 'NURSE_C', 2, 'Given'),
('A111', 'P111', 'MED02', TIMESTAMP '2025-01-18 13:15:00', 'NURSE_B', 1, 'Given'),
('A112', 'P112', 'MED03', TIMESTAMP '2025-01-18 13:45:00', 'NURSE_A', 1, 'Given'),
('A113', 'P113', 'MED01', TIMESTAMP '2025-01-18 14:15:00', 'NURSE_C', 2, 'Given'),
('A114', 'P114', 'MED02', TIMESTAMP '2025-01-18 14:45:00', 'NURSE_B', 1, 'Given'),
('A115', 'P115', 'MED03', TIMESTAMP '2025-01-18 15:15:00', 'NURSE_A', 1, 'Given'),
('A116', 'P116', 'MED01', TIMESTAMP '2025-01-18 15:45:00', 'NURSE_C', 2, 'Given'),
('A117', 'P117', 'MED02', TIMESTAMP '2025-01-18 16:15:00', 'NURSE_B', 1, 'Given'),
('A118', 'P118', 'MED03', TIMESTAMP '2025-01-18 16:45:00', 'NURSE_A', 1, 'Given'),
('A119', 'P119', 'MED01', TIMESTAMP '2025-01-18 17:15:00', 'NURSE_C', 2, 'Given'),
('A120', 'P120', 'MED02', TIMESTAMP '2025-01-18 17:45:00', 'NURSE_B', 1, 'Given'),
('A121', 'P121', 'MED03', TIMESTAMP '2025-01-18 18:15:00', 'NURSE_A', 1, 'Given'),
('A122', 'P122', 'MED01', TIMESTAMP '2025-01-18 18:45:00', 'NURSE_C', 2, 'Given'),
('A123', 'P123', 'MED02', TIMESTAMP '2025-01-18 19:15:00', 'NURSE_B', 1, 'Given'),
('A124', 'P124', 'MED03', TIMESTAMP '2025-01-18 19:45:00', 'NURSE_A', 1, 'Given'),
('A125', 'P125', 'MED01', TIMESTAMP '2025-01-18 20:15:00', 'NURSE_C', 2, 'Given'),
('A126', 'P126', 'MED02', TIMESTAMP '2025-01-18 20:45:00', 'NURSE_B', 1, 'Given'),
('A127', 'P127', 'MED03', TIMESTAMP '2025-01-18 21:15:00', 'NURSE_A', 1, 'Given'),
('A128', 'P128', 'MED01', TIMESTAMP '2025-01-18 21:45:00', 'NURSE_C', 2, 'Given'),
('A129', 'P129', 'MED02', TIMESTAMP '2025-01-18 22:15:00', 'NURSE_B', 1, 'Given'),
('A130', 'P130', 'MED03', TIMESTAMP '2025-01-18 22:45:00', 'NURSE_A', 1, 'Given'),
('A131', 'P131', 'MED01', TIMESTAMP '2025-01-18 23:15:00', 'NURSE_C', 2, 'Given'),
('A132', 'P132', 'MED02', TIMESTAMP '2025-01-18 07:15:00', 'NURSE_B', 1, 'Given'),
('A133', 'P133', 'MED03', TIMESTAMP '2025-01-18 07:45:00', 'NURSE_A', 1, 'Given'),
('A134', 'P134', 'MED01', TIMESTAMP '2025-01-18 06:15:00', 'NURSE_C', 2, 'Given'),
('A135', 'P135', 'MED02', TIMESTAMP '2025-01-18 06:45:00', 'NURSE_B', 1, 'Given'),
('A136', 'P136', 'MED03', TIMESTAMP '2025-01-18 05:15:00', 'NURSE_A', 1, 'Given'),
('A137', 'P137', 'MED01', TIMESTAMP '2025-01-18 05:45:00', 'NURSE_C', 2, 'Given'),
('A138', 'P138', 'MED02', TIMESTAMP '2025-01-18 04:15:00', 'NURSE_B', 1, 'Given'),
('A139', 'P139', 'MED03', TIMESTAMP '2025-01-18 04:45:00', 'NURSE_A', 1, 'Given'),
('A140', 'P140', 'MED01', TIMESTAMP '2025-01-18 03:15:00', 'NURSE_C', 2, 'Given'),
-- D141 is Unmatched
('A142', 'P142', 'MED03', TIMESTAMP '2025-01-18 02:15:00', 'NURSE_A', 1, 'Given'),
('A143', 'P143', 'MED01', TIMESTAMP '2025-01-18 02:45:00', 'NURSE_C', 1, 'Given'), -- Qty Mismatch (Dispense 2, Admin 1)
('A144', 'P144', 'MED02', TIMESTAMP '2025-01-18 01:15:00', 'NURSE_B', 1, 'Given'),
('A145', 'P145', 'MED03', TIMESTAMP '2025-01-18 01:45:00', 'NURSE_A', 1, 'Given'),
('A146', 'P146', 'MED01', TIMESTAMP '2025-01-18 00:15:00', 'NURSE_C', 2, 'Given'),
('A147', 'P147', 'MED02', TIMESTAMP '2025-01-18 00:45:00', 'NURSE_B', 1, 'Given'),
('A148', 'P148', 'MED03', TIMESTAMP '2025-01-18 23:45:00', 'NURSE_A', 1, 'Given'),
('A149', 'P149', 'MED01', TIMESTAMP '2025-01-19 08:30:00', 'NURSE_C', 2, 'Given'),
('A150', 'P150', 'MED02', TIMESTAMP '2025-01-19 09:30:00', 'NURSE_B', 1, 'Given');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Variance Calculation
-------------------------------------------------------------------------------
-- Logic: Join Dispense to Admin on Patient, Med, and nearest time window (+/- 2 hours)
CREATE OR REPLACE VIEW HealthcarePharmaDB.Silver.AdminVariances AS
SELECT
    d.DispenseID,
    d.PatientID,
    d.MedicationID,
    d.DispenseTime,
    d.Quantity AS DispensedQty,
    a.AdminID,
    a.AdminTime,
    a.Quantity AS AdminQty,
    CASE 
        WHEN a.AdminID IS NULL THEN 'Unmatched Dispense'
        WHEN d.Quantity != a.Quantity THEN 'Qty Mismatch'
        WHEN TIMESTAMPDIFF(MINUTE, d.DispenseTime, a.AdminTime) > 60 THEN 'Delayed Admin'
        ELSE 'Matched'
    END AS VarianceType
FROM HealthcarePharmaDB.Bronze.DispensingLogs d
LEFT JOIN HealthcarePharmaDB.Bronze.AdminLogs a
    ON d.PatientID = a.PatientID 
    AND d.MedicationID = a.MedicationID
    AND a.AdminTime BETWEEN d.DispenseTime AND DATE_ADD(d.DispenseTime, 2, 'HOUR'); -- 2 hour window

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Safety Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcarePharmaDB.Gold.MedSafetyReport AS
SELECT
    VarianceType,
    COUNT(*) AS IncidentCount,
    -- List details for high risk
    ARRAY_AGG(DispenseID) AS IncidentIDs
FROM HealthcarePharmaDB.Silver.AdminVariances
WHERE VarianceType != 'Matched'
GROUP BY VarianceType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all Unmatched Dispense events (Potential Diversion)."
    2. "Show incidences of Quantity Mismatches."
    3. "Which User has the most Delayed Admin events?"
*/
