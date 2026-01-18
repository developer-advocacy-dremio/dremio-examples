/*
    Dremio High-Volume SQL Pattern: Healthcare Opioid Monitoring
    
    Business Scenario:
    A Health System wants to monitor controlled substance prescriptions to identify potential
    over-prescribing patterns or patients at risk of overdose.
    
    Data Story:
    We track prescription logs and a master schedule of drugs.
    
    Medallion Architecture:
    - Bronze: Raw ingestion of prescription logs (PrescriptionLogs) and drug data (DrugMaster).
      *Volume*: 50+ records for realistic aggregation.
    - Silver: Enriched data with Morphine Milligram Equivalent (MME) calculations.
    - Gold: Risk alerts for patients exceeding safe daily dosage thresholds.
    
    Key Dremio Features:
    - Date/Time Math (TIMESTAMPDIFF)
    - Analytical Window Functions (SUM OVER)
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareOpioidDB;
CREATE FOLDER IF NOT EXISTS HealthcareOpioidDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareOpioidDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareOpioidDB.Gold;
USE HealthcareOpioidDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareOpioidDB.Bronze.DrugMaster (
    DrugID STRING,
    DrugName STRING,
    DrugClass STRING,
    MME_Conversion_Factor DOUBLE -- Factor to convert dose to Morphine Equivalent
);

INSERT INTO HealthcareOpioidDB.Bronze.DrugMaster VALUES
('D001', 'Morphine Sulfate', 'Opioid Agonist', 1.0),
('D002', 'Oxycodone', 'Opioid Agonist', 1.5),
('D003', 'Hydrocodone', 'Opioid Agonist', 1.0),
('D004', 'Hydromorphone', 'Opioid Agonist', 4.0),
('D005', 'Fentanyl Patch', 'Opioid Agonist', 2.4), -- Simplified conversion for demo
('D006', 'Codeine', 'Opioid Agonist', 0.15),
('D007', 'Tramadol', 'Opioid Agonist', 0.1),
('D008', 'Methadone', 'Opioid Agonist', 3.0), -- Variable, simplified
('D009', 'Ibuprofen', 'NSAID', 0.0),
('D010', 'Acetaminophen', 'Analgesic', 0.0);

CREATE OR REPLACE TABLE HealthcareOpioidDB.Bronze.PrescriptionLogs (
    RxID STRING,
    PatientID STRING,
    PrescriberID STRING,
    DrugID STRING,
    FillDate DATE,
    Quantity INT,
    DaysSupply INT,
    StrengthPerUnit DOUBLE -- mg per pill/patch
);

-- Insert 50+ records representing a mix of safe and high-risk patterns
INSERT INTO HealthcareOpioidDB.Bronze.PrescriptionLogs VALUES
-- Patient P100: Chronic pain, stable
('RX001', 'P100', 'DOC55', 'D001', DATE '2025-01-01', 60, 30, 15.0),
('RX002', 'P100', 'DOC55', 'D001', DATE '2025-02-01', 60, 30, 15.0),
('RX003', 'P100', 'DOC55', 'D001', DATE '2025-03-01', 60, 30, 15.0),
-- Patient P101: Escalating usage, multiple prescribers (Doctor Shopping Risk)
('RX004', 'P101', 'DOC55', 'D002', DATE '2025-01-05', 30, 10, 10.0),
('RX005', 'P101', 'DOC66', 'D002', DATE '2025-01-12', 40, 10, 10.0), -- Early refill?
('RX006', 'P101', 'DOC77', 'D004', DATE '2025-01-20', 20, 10, 4.0), -- Stronger drug
('RX007', 'P101', 'DOC55', 'D002', DATE '2025-02-05', 90, 30, 10.0),
-- Patient P102: Acute injury, short term
('RX008', 'P102', 'DOC88', 'D003', DATE '2025-01-10', 20, 5, 5.0),
('RX009', 'P102', 'DOC88', 'D009', DATE '2025-01-15', 30, 10, 400.0), -- NSAID
-- Patient P103: High dose fentanyl
('RX010', 'P103', 'DOC99', 'D005', DATE '2025-01-01', 10, 30, 25.0), -- 25mcg/hr -> approx calc
('RX011', 'P103', 'DOC99', 'D005', DATE '2025-02-01', 10, 30, 50.0), -- Dose increase
-- Patient P104-P140: Bulk data for aggregation
('RX012', 'P104', 'DOC55', 'D001', DATE '2025-01-02', 30, 15, 15.0),
('RX013', 'P105', 'DOC66', 'D002', DATE '2025-01-03', 30, 15, 10.0),
('RX014', 'P106', 'DOC77', 'D003', DATE '2025-01-04', 30, 15, 5.0),
('RX015', 'P107', 'DOC88', 'D004', DATE '2025-01-05', 30, 15, 2.0),
('RX016', 'P108', 'DOC99', 'D006', DATE '2025-01-06', 30, 15, 30.0),
('RX017', 'P109', 'DOC55', 'D007', DATE '2025-01-07', 30, 15, 50.0),
('RX018', 'P110', 'DOC66', 'D008', DATE '2025-01-08', 30, 15, 5.0),
('RX019', 'P111', 'DOC77', 'D001', DATE '2025-01-09', 60, 30, 15.0),
('RX020', 'P112', 'DOC88', 'D002', DATE '2025-01-10', 60, 30, 10.0),
('RX021', 'P113', 'DOC99', 'D003', DATE '2025-01-11', 60, 30, 5.0),
('RX022', 'P114', 'DOC55', 'D004', DATE '2025-01-12', 60, 30, 2.0),
('RX023', 'P115', 'DOC66', 'D006', DATE '2025-01-13', 60, 30, 30.0),
('RX024', 'P116', 'DOC77', 'D007', DATE '2025-01-14', 60, 30, 50.0),
('RX025', 'P117', 'DOC88', 'D008', DATE '2025-01-15', 60, 30, 5.0),
('RX026', 'P118', 'DOC99', 'D001', DATE '2025-01-16', 90, 30, 15.0),
('RX027', 'P119', 'DOC55', 'D002', DATE '2025-01-17', 90, 30, 10.0),
('RX028', 'P120', 'DOC66', 'D003', DATE '2025-01-18', 90, 30, 5.0),
('RX029', 'P121', 'DOC77', 'D004', DATE '2025-01-19', 90, 30, 2.0),
('RX030', 'P122', 'DOC88', 'D006', DATE '2025-01-20', 90, 30, 30.0),
('RX031', 'P123', 'DOC99', 'D007', DATE '2025-01-21', 90, 30, 50.0),
('RX032', 'P124', 'DOC55', 'D008', DATE '2025-01-22', 90, 30, 5.0),
('RX033', 'P125', 'DOC66', 'D001', DATE '2025-02-01', 30, 15, 15.0),
('RX034', 'P126', 'DOC77', 'D002', DATE '2025-02-02', 30, 15, 10.0),
('RX035', 'P127', 'DOC88', 'D003', DATE '2025-02-03', 30, 15, 5.0),
('RX036', 'P128', 'DOC99', 'D004', DATE '2025-02-04', 30, 15, 2.0),
('RX037', 'P129', 'DOC55', 'D006', DATE '2025-02-05', 30, 15, 30.0),
('RX038', 'P130', 'DOC66', 'D007', DATE '2025-02-06', 30, 15, 50.0),
('RX039', 'P131', 'DOC77', 'D008', DATE '2025-02-07', 30, 15, 5.0),
('RX040', 'P132', 'DOC88', 'D001', DATE '2025-02-08', 60, 30, 15.0),
('RX041', 'P133', 'DOC99', 'D002', DATE '2025-02-09', 60, 30, 10.0),
('RX042', 'P134', 'DOC55', 'D003', DATE '2025-02-10', 60, 30, 5.0),
('RX043', 'P135', 'DOC66', 'D004', DATE '2025-02-11', 60, 30, 2.0),
('RX044', 'P136', 'DOC77', 'D006', DATE '2025-02-12', 60, 30, 30.0),
('RX045', 'P137', 'DOC88', 'D007', DATE '2025-02-13', 60, 30, 50.0),
('RX046', 'P138', 'DOC99', 'D008', DATE '2025-02-14', 60, 30, 5.0),
('RX047', 'P139', 'DOC55', 'D001', DATE '2025-03-01', 30, 15, 15.0),
('RX048', 'P140', 'DOC66', 'D002', DATE '2025-03-02', 30, 15, 10.0),
('RX049', 'P101', 'DOC88', 'D005', DATE '2025-02-28', 10, 10, 50.0), -- P101 back again
('RX050', 'P141', 'DOC99', 'D003', DATE '2025-03-03', 30, 10, 5.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: MME Calculations
-------------------------------------------------------------------------------
-- Calculate total Milligrams and Daily MME for each prescription
-- Formula: (Qty * Strength * ConvFactor) / DaysSupply = Daily MME
CREATE OR REPLACE VIEW HealthcareOpioidDB.Silver.PrescriptionRiskCalc AS
SELECT
    rx.RxID,
    rx.PatientID,
    rx.PrescriberID,
    rx.FillDate,
    d.DrugName,
    rx.Quantity,
    rx.DaysSupply,
    (rx.Quantity * rx.StrengthPerUnit) AS TotalMg,
    d.MME_Conversion_Factor,
    ((rx.Quantity * rx.StrengthPerUnit * d.MME_Conversion_Factor) / rx.DaysSupply) AS Daily_MME,
    CASE WHEN d.DrugClass = 'Opioid Agonist' THEN 1 ELSE 0 END AS IsOpioid,
    -- Calculate end date of supply
    DATE_ADD(rx.FillDate, CAST(rx.DaysSupply AS INT)) AS SupplyEndDate
FROM HealthcareOpioidDB.Bronze.PrescriptionLogs rx
JOIN HealthcareOpioidDB.Bronze.DrugMaster d ON rx.DrugID = d.DrugID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: High Risk Alerts
-------------------------------------------------------------------------------
-- Identify patients who exceed CDC guidelines (>90 MME/day) or have overlaps
-- We aggregate MME by patient for active overlapping periods (simplified here to monthly avg)
CREATE OR REPLACE VIEW HealthcareOpioidDB.Gold.HighRiskPatientAlerts AS
SELECT
    PatientID,
    COUNT(Distinct PrescriberID) AS PrescriberCount,
    COUNT(Distinct RxID) AS OpioidRxCount,
    SUM(Daily_MME) AS Cumulative_Daily_MME, -- Approximation of overlap intensity
    MAX(FillDate) AS LastFillDate,
    CASE 
        WHEN SUM(Daily_MME) >= 90 THEN 'CRITICAL'
        WHEN SUM(Daily_MME) >= 50 THEN 'WARNING'
        ELSE 'MONITOR'
    END AS RiskLevel,
    CASE
        WHEN COUNT(Distinct PrescriberID) >= 3 THEN 'Possible Doctor Shopping'
        ELSE 'Standard'
    END AS BehaviorFlag
FROM HealthcareOpioidDB.Silver.PrescriptionRiskCalc
WHERE IsOpioid = 1
  AND FillDate >= DATE_SUB(CURRENT_DATE, 90) -- Look at last 90 days
GROUP BY PatientID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me the distribution of risk levels for patients in the last 90 days."
    2. "List all patients flagged with Critical risk and their total MME."
    3. "Identify patients visiting 3 or more prescribers."
    4. "Compare the average Daily MME by drug name."
    5. "Show the trend of total opioid prescriptions filled by month."
*/
