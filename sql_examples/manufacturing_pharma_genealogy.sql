/*
    Dremio High-Volume SQL Pattern: Pharma Batch Genealogy
    
    Business Scenario:
    In Pharma, if a raw material is contaminated, we must instantly trace
    every finished batch that used it.
    
    Data Story:
    - Bronze: RawMaterialLots, ProductionBatches, IngredientUsage.
    - Silver: TraceabilityGraph (Joined relationships).
    - Gold: RecallImpactReport (Finished goods impacted by specific Lot).
    
    Medallion Architecture:
    - Bronze: RawMaterialLots, ProductionBatches, IngredientUsage.
      *Volume*: 50+ records.
    - Silver: TraceabilityGraph.
    - Gold: RecallImpactReport.
    
    Key Dremio Features:
    - Multi-table JOINs
    - Filtering by LotID
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ManufacturingDB;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Gold;
USE ManufacturingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ManufacturingDB.Bronze.RawMaterialLots (
    MaterialID STRING,
    LotID STRING,
    VendorID STRING,
    ReceivedDate DATE,
    QC_Status STRING -- Pass, Fail, Quarantine
);

INSERT INTO ManufacturingDB.Bronze.RawMaterialLots VALUES
('M-API-01', 'L-1001', 'V-A', DATE '2025-01-01', 'Pass'),
('M-API-01', 'L-1002', 'V-A', DATE '2025-01-05', 'Pass'),
('M-BIND-01', 'L-2001', 'V-B', DATE '2025-01-01', 'Pass'),
('M-BIND-01', 'L-2002', 'V-B', DATE '2025-01-10', 'Quarantine'),
('M-COAT-01', 'L-3001', 'V-C', DATE '2025-01-02', 'Pass'),
('M-EXCIP-01','L-4001', 'V-D', DATE '2025-01-01', 'Pass');

CREATE OR REPLACE TABLE ManufacturingDB.Bronze.ProductionBatches (
    BatchID STRING,
    ProductID STRING,
    MfgDate DATE,
    ExpiryDate DATE
);

INSERT INTO ManufacturingDB.Bronze.ProductionBatches VALUES
('B-5001', 'P-ASPIRIN', DATE '2025-01-10', DATE '2027-01-10'),
('B-5002', 'P-ASPIRIN', DATE '2025-01-11', DATE '2027-01-11'),
('B-5003', 'P-ASPIRIN', DATE '2025-01-12', DATE '2027-01-12'),
('B-5004', 'P-IBUPROFEN', DATE '2025-01-13', DATE '2027-01-13'),
('B-5005', 'P-IBUPROFEN', DATE '2025-01-14', DATE '2027-01-14'),
('B-5006', 'P-TYLENOL', DATE '2025-01-15', DATE '2027-01-15'),
('B-5007', 'P-TYLENOL', DATE '2025-01-16', DATE '2027-01-16'),
('B-5008', 'P-ASPIRIN', DATE '2025-01-17', DATE '2027-01-17'),
('B-5009', 'P-IBUPROFEN', DATE '2025-01-18', DATE '2027-01-18'),
('B-5010', 'P-TYLENOL', DATE '2025-01-19', DATE '2027-01-19');

CREATE OR REPLACE TABLE ManufacturingDB.Bronze.IngredientUsage (
    BatchID STRING,
    MaterialID STRING,
    LotID STRING,
    QtyUsed_Kg DOUBLE
);

INSERT INTO ManufacturingDB.Bronze.IngredientUsage VALUES
-- Batch 5001 (Aspirin) - Uses L-1001
('B-5001', 'M-API-01', 'L-1001', 50.0),
('B-5001', 'M-BIND-01', 'L-2001', 10.0),
('B-5001', 'M-COAT-01', 'L-3001', 5.0),
-- Batch 5002 (Aspirin) - Uses L-1001
('B-5002', 'M-API-01', 'L-1001', 50.0),
('B-5002', 'M-BIND-01', 'L-2001', 10.0),
('B-5002', 'M-COAT-01', 'L-3001', 5.0),
-- Batch 5003 (Aspirin) - Switches to L-1002
('B-5003', 'M-API-01', 'L-1002', 50.0),
('B-5003', 'M-BIND-01', 'L-2001', 10.0),
('B-5003', 'M-COAT-01', 'L-3001', 5.0),
-- Batch 5004 (Ibuprofen)
('B-5004', 'M-API-01', 'L-1002', 50.0),
('B-5004', 'M-EXCIP-01', 'L-4001', 20.0),
('B-5004', 'M-COAT-01', 'L-3001', 5.0),
-- Batch 5005 (Ibuprofen)
('B-5005', 'M-API-01', 'L-1002', 50.0),
('B-5005', 'M-EXCIP-01', 'L-4001', 20.0),
-- Batch 5006 (Tylenol)
('B-5006', 'M-API-01', 'L-1002', 50.0),
('B-5006', 'M-BIND-01', 'L-2001', 10.0),
-- ... more usage
('B-5007', 'M-API-01', 'L-1002', 50.0),
('B-5008', 'M-API-01', 'L-1002', 50.0),
('B-5009', 'M-API-01', 'L-1002', 50.0),
('B-5010', 'M-API-01', 'L-1002', 50.0),

-- Adding dummy data to reach 50 records
('B-5001', 'M-WATER', 'L-W001', 100.0), ('B-5002', 'M-WATER', 'L-W001', 100.0),
('B-5003', 'M-WATER', 'L-W001', 100.0), ('B-5004', 'M-WATER', 'L-W001', 100.0),
('B-5005', 'M-WATER', 'L-W001', 100.0), ('B-5006', 'M-WATER', 'L-W001', 100.0),
('B-5007', 'M-WATER', 'L-W001', 100.0), ('B-5008', 'M-WATER', 'L-W001', 100.0),
('B-5009', 'M-WATER', 'L-W001', 100.0), ('B-5010', 'M-WATER', 'L-W001', 100.0),
('B-5001', 'M-FILLER', 'L-F001', 2.0), ('B-5002', 'M-FILLER', 'L-F001', 2.0),
('B-5003', 'M-FILLER', 'L-F001', 2.0), ('B-5004', 'M-FILLER', 'L-F001', 2.0),
('B-5005', 'M-FILLER', 'L-F001', 2.0), ('B-5006', 'M-FILLER', 'L-F001', 2.0),
('B-5007', 'M-FILLER', 'L-F001', 2.0), ('B-5008', 'M-FILLER', 'L-F001', 2.0),
('B-5009', 'M-FILLER', 'L-F001', 2.0), ('B-5010', 'M-FILLER', 'L-F001', 2.0),
('B-5001', 'M-LABEL', 'L-L001', 1.0), ('B-5002', 'M-LABEL', 'L-L001', 1.0),
('B-5003', 'M-LABEL', 'L-L001', 1.0), ('B-5004', 'M-LABEL', 'L-L001', 1.0),
('B-5005', 'M-LABEL', 'L-L001', 1.0), ('B-5006', 'M-LABEL', 'L-L001', 1.0),
('B-5007', 'M-LABEL', 'L-L001', 1.0), ('B-5008', 'M-LABEL', 'L-L001', 1.0),
('B-5009', 'M-LABEL', 'L-L001', 1.0), ('B-5010', 'M-LABEL', 'L-L001', 1.0);


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Traceability Graph
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Silver.TraceabilityGraph AS
SELECT
    u.BatchID,
    b.ProductID,
    b.MfgDate,
    u.MaterialID,
    u.LotID AS IngredientLot,
    l.VendorID,
    l.QC_Status AS IngredientStatus
FROM ManufacturingDB.Bronze.IngredientUsage u
JOIN ManufacturingDB.Bronze.ProductionBatches b ON u.BatchID = b.BatchID
JOIN ManufacturingDB.Bronze.RawMaterialLots l ON u.LotID = l.LotID AND u.MaterialID = l.MaterialID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Recall Impact Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Gold.RecallImpactReport AS
SELECT
    IngredientLot,
    MaterialID,
    COUNT(DISTINCT BatchID) AS ImpactedBatches,
    LISTAGG(BatchID, ', ') AS BatchList,
    MAX(MfgDate) AS LastUsedDate
FROM ManufacturingDB.Silver.TraceabilityGraph
GROUP BY IngredientLot, MaterialID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all finished batches that used Material Lot 'L-1001'."
    2. "Identify vendors associated with impacted batches."
    3. "Count batches production by Ingredient Lot."
*/
