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
('P005', 'Lung Cancer', 'IV', DATE '2025-01-05');
-- Insert more diagnoses for bulk data
INSERT INTO HealthcareOncologyDB.Bronze.PatientDiagnosis 
SELECT 
  'P' || CAST(rn + 5 AS STRING), 
  CASE WHEN rn % 3 = 0 THEN 'Breast Cancer' WHEN rn % 3 = 1 THEN 'Lung Cancer' ELSE 'Colon Cancer' END,
  CASE WHEN rn % 2 = 0 THEN 'II' ELSE 'IV' END,
  DATE_ADD(DATE '2025-01-01', rn)
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);


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
-- Bulk orders matching the bulk diagnoses roughly
('ORD006', 'P6', 'DOC_A', 'Carbo-Pem', DATE '2025-01-20'),
('ORD007', 'P7', 'DOC_B', 'FOLFIRI', DATE '2025-01-21'),
('ORD008', 'P8', 'DOC_C', 'AC-T', DATE '2025-01-22'),
('ORD009', 'P9', 'DOC_A', 'Carbo-Pem', DATE '2025-01-23'),
('ORD010', 'P10', 'DOC_B', 'FOLFOX', DATE '2025-01-24'),
('ORD011', 'P11', 'DOC_C', 'AC-T', DATE '2025-01-25'),
('ORD012', 'P12', 'DOC_A', 'Carbo-Pem', DATE '2025-01-26'),
('ORD013', 'P13', 'DOC_B', 'FOLFIRI', DATE '2025-01-27'),
('ORD014', 'P14', 'DOC_C', 'AC-T', DATE '2025-01-28'),
('ORD015', 'P15', 'DOC_A', 'Carbo-Pem', DATE '2025-01-29'),
('ORD016', 'P16', 'DOC_B', 'FOLFOX', DATE '2025-01-30'),
('ORD017', 'P17', 'DOC_C', 'AC-T', DATE '2025-01-31'),
('ORD018', 'P18', 'DOC_A', 'Carbo-Pem', DATE '2025-02-01'),
('ORD019', 'P19', 'DOC_B', 'FOLFIRI', DATE '2025-02-02'),
('ORD020', 'P20', 'DOC_C', 'AC-T', DATE '2025-02-03'),
('ORD021', 'P21', 'DOC_A', 'Carbo-Pem', DATE '2025-02-04'),
('ORD022', 'P22', 'DOC_B', 'FOLFOX', DATE '2025-02-05'),
('ORD023', 'P23', 'DOC_C', 'AC-T', DATE '2025-02-06'),
('ORD024', 'P24', 'DOC_A', 'Carbo-Pem', DATE '2025-02-07'),
('ORD025', 'P25', 'DOC_B', 'FOLFIRI', DATE '2025-02-08'),
('ORD026', 'P26', 'DOC_C', 'AC-T', DATE '2025-02-09'),
('ORD027', 'P27', 'DOC_A', 'Carbo-Pem', DATE '2025-02-10'),
('ORD028', 'P28', 'DOC_B', 'FOLFOX', DATE '2025-02-11'),
('ORD029', 'P29', 'DOC_C', 'AC-T', DATE '2025-02-12'),
('ORD030', 'P30', 'DOC_A', 'Carbo-Pem', DATE '2025-02-13'),
('ORD031', 'P31', 'DOC_B', 'FOLFIRI', DATE '2025-02-14'),
('ORD032', 'P32', 'DOC_C', 'AC-T', DATE '2025-02-15'),
('ORD033', 'P33', 'DOC_A', 'Carbo-Pem', DATE '2025-02-16'),
('ORD034', 'P34', 'DOC_B', 'FOLFOX', DATE '2025-02-17'),
('ORD035', 'P35', 'DOC_C', 'AC-T', DATE '2025-02-18'),
('ORD036', 'P36', 'DOC_A', 'Carbo-Pem', DATE '2025-02-19'),
('ORD037', 'P37', 'DOC_B', 'FOLFIRI', DATE '2025-02-20'),
('ORD038', 'P38', 'DOC_C', 'AC-T', DATE '2025-02-21'),
('ORD039', 'P39', 'DOC_A', 'Carbo-Pem', DATE '2025-02-22'),
('ORD040', 'P40', 'DOC_B', 'FOLFOX', DATE '2025-02-23'),
('ORD041', 'P41', 'DOC_C', 'AC-T', DATE '2025-02-24'),
('ORD042', 'P42', 'DOC_A', 'Carbo-Pem', DATE '2025-02-25'),
('ORD043', 'P43', 'DOC_B', 'FOLFIRI', DATE '2025-02-26'),
('ORD044', 'P44', 'DOC_C', 'AC-T', DATE '2025-02-27'),
('ORD045', 'P45', 'DOC_A', 'Carbo-Pem', DATE '2025-02-28'),
('ORD046', 'P46', 'DOC_B', 'FOLFOX', DATE '2025-03-01'),
('ORD047', 'P47', 'DOC_C', 'AC-T', DATE '2025-03-02'),
('ORD048', 'P48', 'DOC_A', 'Carbo-Pem', DATE '2025-03-03'),
('ORD049', 'P49', 'DOC_B', 'FOLFIRI', DATE '2025-03-04'),
('ORD050', 'P50', 'DOC_C', 'AC-T', DATE '2025-03-05');

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
