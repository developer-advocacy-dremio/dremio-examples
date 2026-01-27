/*
 * Chemical Manufacturing Batch Traceability Demo
 * 
 * Scenario:
 * Tracking ingredient lots, lab results, and yield analysis for quality control.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ManufacturingDB;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Chemicals;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Chemicals.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Chemicals.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Chemicals.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ManufacturingDB.Chemicals.Bronze.BatchLogs (
    BatchID INT,
    ProductID VARCHAR,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    InputKg DOUBLE,
    OutputKg DOUBLE,
    LabPurityPct DOUBLE
);

INSERT INTO ManufacturingDB.Chemicals.Bronze.BatchLogs VALUES
(1, 'PolymX', '2025-01-01 08:00:00', '2025-01-01 12:00:00', 1000.0, 950.0, 99.5),
(2, 'PolymX', '2025-01-01 13:00:00', '2025-01-01 17:00:00', 1000.0, 920.0, 98.0),
(3, 'SolvA', '2025-01-01 08:00:00', '2025-01-01 10:00:00', 500.0, 490.0, 99.9),
(4, 'SolvA', '2025-01-01 11:00:00', '2025-01-01 13:00:00', 500.0, 480.0, 99.1),
(5, 'PolymY', '2025-01-02 08:00:00', '2025-01-02 16:00:00', 2000.0, 1900.0, 97.5),
(6, 'PolymX', '2025-01-02 08:00:00', '2025-01-02 12:00:00', 1000.0, 960.0, 99.2),
(7, 'SolvB', '2025-01-02 09:00:00', '2025-01-02 11:00:00', 600.0, 580.0, 99.8),
(8, 'PolymY', '2025-01-03 08:00:00', '2025-01-03 16:00:00', 2000.0, 1850.0, 96.0), -- Low yield
(9, 'SolvA', '2025-01-03 08:00:00', '2025-01-03 10:00:00', 500.0, 495.0, 99.9),
(10, 'SolvB', '2025-01-03 10:00:00', '2025-01-03 12:00:00', 600.0, 590.0, 99.7);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ManufacturingDB.Chemicals.Silver.QualityControl AS
SELECT 
    BatchID,
    ProductID,
    OutputKg,
    (OutputKg / InputKg) * 100.0 AS YieldPct,
    LabPurityPct,
    CASE 
        WHEN LabPurityPct < 99.0 OR (OutputKg / InputKg) < 0.95 THEN 'Review'
        ELSE 'Pass'
    END AS QC_Status
FROM ManufacturingDB.Chemicals.Bronze.BatchLogs;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ManufacturingDB.Chemicals.Gold.ProductEfficiency AS
SELECT 
    ProductID,
    COUNT(*) AS BatchCount,
    AVG(YieldPct) AS AvgYield,
    AVG(LabPurityPct) AS AvgPurity,
    SUM(CASE WHEN QC_Status = 'Review' THEN 1 ELSE 0 END) AS FailedBatches
FROM ManufacturingDB.Chemicals.Silver.QualityControl
GROUP BY ProductID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all products wth an AvgYield below 95% in ManufacturingDB.Chemicals.Gold.ProductEfficiency."
*/
