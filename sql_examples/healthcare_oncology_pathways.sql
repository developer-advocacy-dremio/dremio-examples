/*
    Dremio High-Volume SQL Pattern: Healthcare Oncology Treatment Pathways
    
    Business Scenario:
    Cancer centers use standard "Clinical Pathways" (NCCN Guidelines) to ensure consistent care.
    Variances from these pathways need to be tracked for quality assurance and insurance authorization.
    
    Data Story:
    We track patient diagnosis (ICD-10) and the specific chemotherapy orders placed.
    We compare these against a "Pathways Master" table.
    
    Medallion Architecture:
    - Bronze: PatientDiagnosis, ChemoOrders, CarePathwaysMaster.
      *Volume*: 50+ records.
    - Silver: Enriched orders showing matched pathways.
    - Gold: Variance Analysis (Off-pathway orders).
    
    Key Dremio Features:
    - LEFT JOIN to find mismatches
    - Aggregation by Provider
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareOncologyDB;
CREATE FOLDER IF NOT EXISTS HealthcareOncologyDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareOncologyDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareOncologyDB.Gold;
USE HealthcareOncologyDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareOncologyDB.Bronze.CarePathwaysMaster (
    PathwayID STRING,
    CancerType STRING, -- Breast, Lung, Colon
    Stage STRING, -- I, II, III, IV
    RegimenName STRING,
    DrugsIncluded STRING
);

INSERT INTO HealthcareOncologyDB.Bronze.CarePathwaysMaster VALUES
('PATH001', 'Breast Cancer', 'II', 'AC-T', 'Doxorubicin, Cyclophosphamide, Paclitaxel'),
('PATH002', 'Breast Cancer', 'III', 'AC-T', 'Doxorubicin, Cyclophosphamide, Paclitaxel'),
('PATH003', 'Lung Cancer', 'IV', 'Carbo-Pem', 'Carboplatin, Pemetrexed, Pembrolizumab'),
('PATH004', 'Colon Cancer', 'III', 'FOLFOX', 'Folinic Acid, Fluorouracil, Oxaliplatin'),
('PATH005', 'Colon Cancer', 'IV', 'FOLFIRI', 'Folinic Acid, Fluorouracil, Irinotecan');

CREATE OR REPLACE TABLE HealthcareOncologyDB.Bronze.PatientDiagnosis (
    PatientID STRING,
    CancerType STRING,
    Stage STRING,
    DiagnosisDate DATE
);

INSERT INTO HealthcareOncologyDB.Bronze.PatientDiagnosis VALUES
('P001', 'Breast Cancer', 'II', DATE '2025-01-01'),
('P002', 'Lung Cancer', 'IV', DATE '2025-01-02'),
('P003', 'Colon Cancer', 'III', DATE '2025-01-03'),
('P004', 'Breast Cancer', 'II', DATE '2025-01-04'),
('P005', 'Lung Cancer', 'IV', DATE '2025-01-05'),
('P006', 'Breast Cancer', 'II', DATE '2025-01-06'),
('P007', 'Lung Cancer', 'IV', DATE '2025-01-07'),
('P008', 'Colon Cancer', 'III', DATE '2025-01-08'),
('P009', 'Breast Cancer', 'II', DATE '2025-01-09'),
('P010', 'Lung Cancer', 'IV', DATE '2025-01-10'),
('P011', 'Breast Cancer', 'II', DATE '2025-01-11'),
('P012', 'Lung Cancer', 'IV', DATE '2025-01-12'),
('P013', 'Colon Cancer', 'III', DATE '2025-01-13'),
('P014', 'Breast Cancer', 'II', DATE '2025-01-14'),
('P015', 'Lung Cancer', 'IV', DATE '2025-01-15'),
('P016', 'Breast Cancer', 'II', DATE '2025-01-16'),
('P017', 'Lung Cancer', 'IV', DATE '2025-01-17'),
('P018', 'Colon Cancer', 'III', DATE '2025-01-18'),
('P019', 'Breast Cancer', 'II', DATE '2025-01-19'),
('P020', 'Lung Cancer', 'IV', DATE '2025-01-20'),
('P021', 'Breast Cancer', 'II', DATE '2025-01-21'),
('P022', 'Lung Cancer', 'IV', DATE '2025-01-22'),
('P023', 'Colon Cancer', 'III', DATE '2025-01-23'),
('P024', 'Breast Cancer', 'II', DATE '2025-01-24'),
('P025', 'Lung Cancer', 'IV', DATE '2025-01-25'),
('P026', 'Breast Cancer', 'II', DATE '2025-01-26'),
('P027', 'Lung Cancer', 'IV', DATE '2025-01-27'),
('P028', 'Colon Cancer', 'III', DATE '2025-01-28'),
('P029', 'Breast Cancer', 'II', DATE '2025-01-29'),
('P030', 'Lung Cancer', 'IV', DATE '2025-01-30'),
('P031', 'Breast Cancer', 'II', DATE '2025-01-31'),
('P032', 'Lung Cancer', 'IV', DATE '2025-02-01'),
('P033', 'Colon Cancer', 'III', DATE '2025-02-02'),
('P034', 'Breast Cancer', 'II', DATE '2025-02-03'),
('P035', 'Lung Cancer', 'IV', DATE '2025-02-04'),
('P036', 'Breast Cancer', 'II', DATE '2025-02-05'),
('P037', 'Lung Cancer', 'IV', DATE '2025-02-06'),
('P038', 'Colon Cancer', 'III', DATE '2025-02-07'),
('P039', 'Breast Cancer', 'II', DATE '2025-02-08'),
('P040', 'Lung Cancer', 'IV', DATE '2025-02-09'),
('P041', 'Breast Cancer', 'II', DATE '2025-02-10'),
('P042', 'Lung Cancer', 'IV', DATE '2025-02-11'),
('P043', 'Colon Cancer', 'III', DATE '2025-02-12'),
('P044', 'Breast Cancer', 'II', DATE '2025-02-13'),
('P045', 'Lung Cancer', 'IV', DATE '2025-02-14'),
('P046', 'Breast Cancer', 'II', DATE '2025-02-15'),
('P047', 'Lung Cancer', 'IV', DATE '2025-02-16'),
('P048', 'Colon Cancer', 'III', DATE '2025-02-17'),
('P049', 'Breast Cancer', 'II', DATE '2025-02-18'),
('P050', 'Lung Cancer', 'IV', DATE '2025-02-19');

CREATE OR REPLACE TABLE HealthcareOncologyDB.Bronze.ChemoOrders (
    OrderID STRING,
    PatientID STRING,
    OrderingProvider STRING,
    RegimenOrdered STRING,
    OrderDate DATE
);

INSERT INTO HealthcareOncologyDB.Bronze.ChemoOrders VALUES
('ORD001', 'P001', 'DOC_A', 'AC-T', DATE '2025-01-10'), -- On Pathway
('ORD002', 'P002', 'DOC_B', 'Carbo-Pem', DATE '2025-01-12'), -- On Pathway
('ORD003', 'P003', 'DOC_C', 'FOLFIRI', DATE '2025-01-15'), -- Off Pathway (Should be FOLFOX for Stage III)
('ORD004', 'P004', 'DOC_A', 'AC-T', DATE '2025-01-16'),
('ORD005', 'P005', 'DOC_B', 'Cisplatin-Etoposide', DATE '2025-01-18'), -- Off Pathway
('ORD006', 'P006', 'DOC_A', 'AC-T', DATE '2025-01-20'),
('ORD007', 'P007', 'DOC_B', 'Carbo-Pem', DATE '2025-01-21'),
('ORD008', 'P008', 'DOC_C', 'FOLFOX', DATE '2025-01-22'),
('ORD009', 'P009', 'DOC_A', 'AC-T', DATE '2025-01-23'),
('ORD010', 'P010', 'DOC_B', 'Carbo-Pem', DATE '2025-01-24'),
('ORD011', 'P011', 'DOC_C', 'AC-T', DATE '2025-01-25'),
('ORD012', 'P012', 'DOC_A', 'Carbo-Pem', DATE '2025-01-26'),
('ORD013', 'P013', 'DOC_B', 'FOLFOX', DATE '2025-01-27'),
('ORD014', 'P014', 'DOC_C', 'AC-T', DATE '2025-01-28'),
('ORD015', 'P015', 'DOC_A', 'Carbo-Pem', DATE '2025-01-29'),
('ORD016', 'P016', 'DOC_B', 'AC-T', DATE '2025-01-30'),
('ORD017', 'P017', 'DOC_C', 'Carbo-Pem', DATE '2025-01-31'),
('ORD018', 'P018', 'DOC_A', 'FOLFIRI', DATE '2025-02-01'), -- Variance
('ORD019', 'P019', 'DOC_B', 'AC-T', DATE '2025-02-02'),
('ORD020', 'P020', 'DOC_C', 'Carbo-Pem', DATE '2025-02-03'),
('ORD021', 'P021', 'DOC_A', 'AC-T', DATE '2025-02-04'),
('ORD022', 'P022', 'DOC_B', 'Carbo-Pem', DATE '2025-02-05'),
('ORD023', 'P023', 'DOC_C', 'FOLFOX', DATE '2025-02-06'),
('ORD024', 'P024', 'DOC_A', 'AC-T', DATE '2025-02-07'),
('ORD025', 'P025', 'DOC_B', 'Carbo-Pem', DATE '2025-02-08'),
('ORD026', 'P026', 'DOC_C', 'AC-T', DATE '2025-02-09'),
('ORD027', 'P027', 'DOC_A', 'Cisplatin-Etoposide', DATE '2025-02-10'), -- Variance
('ORD028', 'P028', 'DOC_B', 'FOLFOX', DATE '2025-02-11'),
('ORD029', 'P029', 'DOC_C', 'AC-T', DATE '2025-02-12'),
('ORD030', 'P030', 'DOC_A', 'Carbo-Pem', DATE '2025-02-13'),
('ORD031', 'P031', 'DOC_B', 'AC-T', DATE '2025-02-14'),
('ORD032', 'P032', 'DOC_C', 'Carbo-Pem', DATE '2025-02-15'),
('ORD033', 'P033', 'DOC_A', 'FOLFOX', DATE '2025-02-16'),
('ORD034', 'P034', 'DOC_B', 'AC-T', DATE '2025-02-17'),
('ORD035', 'P035', 'DOC_C', 'Carbo-Pem', DATE '2025-02-18'),
('ORD036', 'P036', 'DOC_A', 'AC-T', DATE '2025-02-19'),
('ORD037', 'P037', 'DOC_B', 'Carbo-Pem', DATE '2025-02-20'),
('ORD038', 'P038', 'DOC_C', 'FOLFOX', DATE '2025-02-21'),
('ORD039', 'P039', 'DOC_A', 'AC-T', DATE '2025-02-22'),
('ORD040', 'P040', 'DOC_B', 'Carbo-Pem', DATE '2025-02-23'),
('ORD041', 'P041', 'DOC_C', 'AC-T', DATE '2025-02-24'),
('ORD042', 'P042', 'DOC_A', 'Carbo-Pem', DATE '2025-02-25'),
('ORD043', 'P043', 'DOC_B', 'FOLFOX', DATE '2025-02-26'),
('ORD044', 'P044', 'DOC_C', 'AC-T', DATE '2025-02-27'),
('ORD045', 'P045', 'DOC_A', 'Carbo-Pem', DATE '2025-02-28'),
('ORD046', 'P046', 'DOC_B', 'AC-T', DATE '2025-03-01'),
('ORD047', 'P047', 'DOC_C', 'Carbo-Pem', DATE '2025-03-02'),
('ORD048', 'P048', 'DOC_A', 'FOLFIRI', DATE '2025-03-03'), -- Variance
('ORD049', 'P049', 'DOC_B', 'AC-T', DATE '2025-03-04'),
('ORD050', 'P050', 'DOC_C', 'Carbo-Pem', DATE '2025-03-05');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Protocol Compliance
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareOncologyDB.Silver.OrderCompliance AS
SELECT
    o.OrderID,
    o.PatientID,
    o.OrderingProvider,
    d.CancerType,
    d.Stage,
    o.RegimenOrdered,
    pm.RegimenName AS ProtocolRegimen,
    CASE 
        WHEN o.RegimenOrdered = pm.RegimenName THEN 'Compliant'
        ELSE 'Variance'
    END AS ComplianceStatus
FROM HealthcareOncologyDB.Bronze.ChemoOrders o
JOIN HealthcareOncologyDB.Bronze.PatientDiagnosis d ON o.PatientID = d.PatientID
LEFT JOIN HealthcareOncologyDB.Bronze.CarePathwaysMaster pm 
    ON d.CancerType = pm.CancerType AND d.Stage = pm.Stage;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Variance Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareOncologyDB.Gold.TreatmentVarianceAnalysis AS
SELECT
    OrderingProvider,
    COUNT(OrderID) AS TotalOrders,
    SUM(CASE WHEN ComplianceStatus = 'Compliant' THEN 1 ELSE 0 END) AS CompliantOrders,
    SUM(CASE WHEN ComplianceStatus = 'Variance' THEN 1 ELSE 0 END) AS VarianceOrders,
    CAST(SUM(CASE WHEN ComplianceStatus = 'Compliant' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(OrderID) * 100 AS ComplianceRate
FROM HealthcareOncologyDB.Silver.OrderCompliance
GROUP BY OrderingProvider;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show the overall pathway compliance rate by provider."
    2. "List all orders flagged as Variance."
    3. "Which Cancer Types have the most off-pathway orders?"
    4. "Compare compliance rates between Breast Cancer and Lung Cancer."
*/
