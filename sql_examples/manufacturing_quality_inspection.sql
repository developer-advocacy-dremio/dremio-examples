/*
    Dremio High-Volume SQL Pattern: Manufacturing Quality Inspection
    
    Business Scenario:
    A smart factory uses Computer Vision (AI) to inspect products on the assembly line.
    We need to track defect rates, false positives (low confidence), and identifying
    problematic production lines.
    
    Data Story:
    - We ingest a stream of 'InspectionLogs' from the vision system.
    - We join this with 'ProductMetadata' to know what was being made.
    
    Medallion Architecture:
    - Bronze: InspectionLogs, ProductMetadata.
      *Volume*: 50+ records simulating high-speed line data.
    - Silver: ValidatedDefects (Filters out low-confidence AI reads).
    - Gold: DefectRateByLine (Aggregated quality metrics).
    
    Key Dremio Features:
    - DOUBLE Precision for Confidence Scores
    - Conditional Aggregation
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
CREATE OR REPLACE TABLE ManufacturingDB.Bronze.ProductMetadata (
    ProductID STRING,
    ProductName STRING,
    Category STRING,
    TargetWeight_g DOUBLE
);

INSERT INTO ManufacturingDB.Bronze.ProductMetadata VALUES
('P101', 'Widget A', 'Electronics', 150.5),
('P102', 'Widget B', 'Electronics', 200.0),
('P103', 'Gear X', 'Mechanical', 500.0),
('P104', 'Gear Y', 'Mechanical', 750.0),
('P105', 'Housing Case', 'Plastic', 50.0);

CREATE OR REPLACE TABLE ManufacturingDB.Bronze.InspectionLogs (
    ImageID STRING,
    InspectionTime TIMESTAMP,
    LineID STRING,
    ProductID STRING,
    DefectDetected BOOLEAN,
    DefectType STRING, -- Scratch, Dent, Misaligned, None
    AI_Confidence DOUBLE -- 0.0 to 1.0
);

-- Simulating 50+ inspection events
INSERT INTO ManufacturingDB.Bronze.InspectionLogs VALUES
-- Line 1 (Running well)
('IMG001', TIMESTAMP '2025-01-20 08:00:01', 'L1', 'P101', FALSE, 'None', 0.99),
('IMG002', TIMESTAMP '2025-01-20 08:00:05', 'L1', 'P101', FALSE, 'None', 0.98),
('IMG003', TIMESTAMP '2025-01-20 08:00:10', 'L1', 'P101', TRUE, 'Scratch', 0.95),
('IMG004', TIMESTAMP '2025-01-20 08:00:15', 'L1', 'P101', FALSE, 'None', 0.99),
('IMG005', TIMESTAMP '2025-01-20 08:00:20', 'L1', 'P101', FALSE, 'None', 0.97),
('IMG006', TIMESTAMP '2025-01-20 08:00:25', 'L1', 'P101', FALSE, 'None', 0.98),
('IMG007', TIMESTAMP '2025-01-20 08:00:30', 'L1', 'P101', FALSE, 'None', 0.99),
('IMG008', TIMESTAMP '2025-01-20 08:00:35', 'L1', 'P101', TRUE, 'Dent', 0.85),
('IMG009', TIMESTAMP '2025-01-20 08:00:40', 'L1', 'P101', FALSE, 'None', 0.99),
('IMG010', TIMESTAMP '2025-01-20 08:00:45', 'L1', 'P101', FALSE, 'None', 0.98),

-- Line 2 (Having issues with alignment)
('IMG011', TIMESTAMP '2025-01-20 08:05:00', 'L2', 'P103', TRUE, 'Misaligned', 0.92),
('IMG012', TIMESTAMP '2025-01-20 08:05:05', 'L2', 'P103', TRUE, 'Misaligned', 0.93),
('IMG013', TIMESTAMP '2025-01-20 08:05:10', 'L2', 'P103', FALSE, 'None', 0.95),
('IMG014', TIMESTAMP '2025-01-20 08:05:15', 'L2', 'P103', TRUE, 'Misaligned', 0.91),
('IMG015', TIMESTAMP '2025-01-20 08:05:20', 'L2', 'P103', TRUE, 'Scratch', 0.60), -- Low conf
('IMG016', TIMESTAMP '2025-01-20 08:05:25', 'L2', 'P103', FALSE, 'None', 0.96),
('IMG017', TIMESTAMP '2025-01-20 08:05:30', 'L2', 'P103', TRUE, 'Misaligned', 0.94),
('IMG018', TIMESTAMP '2025-01-20 08:05:35', 'L2', 'P103', FALSE, 'None', 0.98),
('IMG019', TIMESTAMP '2025-01-20 08:05:40', 'L2', 'P103', TRUE, 'Misaligned', 0.95),
('IMG020', TIMESTAMP '2025-01-20 08:05:45', 'L2', 'P103', FALSE, 'None', 0.97),

-- Line 3 (Mixed production)
('IMG021', TIMESTAMP '2025-01-20 09:00:00', 'L3', 'P105', FALSE, 'None', 0.99),
('IMG022', TIMESTAMP '2025-01-20 09:00:05', 'L3', 'P105', FALSE, 'None', 0.98),
('IMG023', TIMESTAMP '2025-01-20 09:00:10', 'L3', 'P105', FALSE, 'None', 0.99),
('IMG024', TIMESTAMP '2025-01-20 09:00:15', 'L3', 'P105', TRUE, 'Dent', 0.88),
('IMG025', TIMESTAMP '2025-01-20 09:00:20', 'L3', 'P105', FALSE, 'None', 0.97),
('IMG026', TIMESTAMP '2025-01-20 09:00:25', 'L3', 'P105', TRUE, 'Scratch', 0.55), -- Low conf
('IMG027', TIMESTAMP '2025-01-20 09:00:30', 'L3', 'P105', FALSE, 'None', 0.99),
('IMG028', TIMESTAMP '2025-01-20 09:00:35', 'L3', 'P105', FALSE, 'None', 0.96),
('IMG029', TIMESTAMP '2025-01-20 09:00:40', 'L3', 'P105', FALSE, 'None', 0.98),
('IMG030', TIMESTAMP '2025-01-20 09:00:45', 'L3', 'P105', TRUE, 'Dent', 0.90),

-- Line 1 Continued
('IMG031', TIMESTAMP '2025-01-20 08:01:00', 'L1', 'P102', FALSE, 'None', 0.99),
('IMG032', TIMESTAMP '2025-01-20 08:01:05', 'L1', 'P102', FALSE, 'None', 0.99),
('IMG033', TIMESTAMP '2025-01-20 08:01:10', 'L1', 'P102', FALSE, 'None', 0.98),
('IMG034', TIMESTAMP '2025-01-20 08:01:15', 'L1', 'P102', TRUE, 'Scratch', 0.92),
('IMG035', TIMESTAMP '2025-01-20 08:01:20', 'L1', 'P102', FALSE, 'None', 0.99),
('IMG036', TIMESTAMP '2025-01-20 08:01:25', 'L1', 'P102', FALSE, 'None', 0.97),
('IMG037', TIMESTAMP '2025-01-20 08:01:30', 'L1', 'P102', FALSE, 'None', 0.98),
('IMG038', TIMESTAMP '2025-01-20 08:01:35', 'L1', 'P102', FALSE, 'None', 0.99),
('IMG039', TIMESTAMP '2025-01-20 08:01:40', 'L1', 'P102', TRUE, 'Dent', 0.89),
('IMG040', TIMESTAMP '2025-01-20 08:01:45', 'L1', 'P102', FALSE, 'None', 0.99),

-- Random Spot checks
('IMG041', TIMESTAMP '2025-01-20 10:00:00', 'L2', 'P104', FALSE, 'None', 0.99),
('IMG042', TIMESTAMP '2025-01-20 10:00:05', 'L2', 'P104', FALSE, 'None', 0.98),
('IMG043', TIMESTAMP '2025-01-20 10:00:10', 'L2', 'P104', FALSE, 'None', 0.99),
('IMG044', TIMESTAMP '2025-01-20 10:00:15', 'L2', 'P104', FALSE, 'None', 0.97),
('IMG045', TIMESTAMP '2025-01-20 10:00:20', 'L2', 'P104', FALSE, 'None', 0.98),
('IMG046', TIMESTAMP '2025-01-20 10:00:25', 'L2', 'P104', TRUE, 'Misaligned', 0.94),
('IMG047', TIMESTAMP '2025-01-20 10:00:30', 'L2', 'P104', FALSE, 'None', 0.99),
('IMG048', TIMESTAMP '2025-01-20 10:00:35', 'L2', 'P104', FALSE, 'None', 0.96),
('IMG049', TIMESTAMP '2025-01-20 10:00:40', 'L2', 'P104', FALSE, 'None', 0.98),
('IMG050', TIMESTAMP '2025-01-20 10:00:45', 'L2', 'P104', FALSE, 'None', 0.99),
('IMG051', TIMESTAMP '2025-01-20 10:01:00', 'L2', 'P104', TRUE, 'Scratch', 0.75);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Validated Defects
-------------------------------------------------------------------------------
-- Filter out low-confidence detections (< 80%) to avoid false alarms
CREATE OR REPLACE VIEW ManufacturingDB.Silver.ValidatedDefects AS
SELECT
    i.ImageID,
    i.InspectionTime,
    i.LineID,
    i.ProductID,
    p.ProductName,
    i.DefectType,
    i.AI_Confidence
FROM ManufacturingDB.Bronze.InspectionLogs i
JOIN ManufacturingDB.Bronze.ProductMetadata p ON i.ProductID = p.ProductID
WHERE i.DefectDetected = TRUE
  AND i.AI_Confidence >= 0.80;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Defect Rate by Line
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Gold.DefectRateByLine AS
SELECT
    LineID,
    COUNT(*) AS TotalInspections,
    SUM(CASE WHEN DefectDetected = TRUE AND AI_Confidence >= 0.80 THEN 1 ELSE 0 END) AS ConfirmedDefects,
    SUM(CASE WHEN DefectDetected = TRUE AND AI_Confidence < 0.80 THEN 1 ELSE 0 END) AS LowConfFlags,
    (CAST(SUM(CASE WHEN DefectDetected = TRUE AND AI_Confidence >= 0.80 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) * 100.0 AS DefectRatePct
FROM ManufacturingDB.Bronze.InspectionLogs
GROUP BY LineID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which production line has the highest defect rate?"
    2. "Show me the distribution of Defect Types for Product 'Widget A'."
    3. "List all low-confidence defect detections for manual review."
*/
