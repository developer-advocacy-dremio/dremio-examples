/*
 * Biotech Lab Research Demo
 * 
 * Scenario:
 * Tracking experiment results, reagent usage, and equipment maintenance in a research lab.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Improve experiment reproducibility and supply chain management.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS BiotechDB;
CREATE FOLDER IF NOT EXISTS BiotechDB.Bronze;
CREATE FOLDER IF NOT EXISTS BiotechDB.Silver;
CREATE FOLDER IF NOT EXISTS BiotechDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS BiotechDB.Bronze.Experiments (
    ExpID VARCHAR,
    Researcher VARCHAR,
    StartDate DATE,
    ProtocolID VARCHAR,
    Status VARCHAR -- 'Planned', 'In Progress', 'Completed', 'Failed'
);

CREATE TABLE IF NOT EXISTS BiotechDB.Bronze.LabResults (
    ResultID INT,
    ExpID VARCHAR,
    MetricName VARCHAR,
    MetricValue DOUBLE,
    "Timestamp" TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BiotechDB.Bronze.Inventory (
    ItemID INT,
    Name VARCHAR,
    Category VARCHAR, -- 'Reagent', 'Antibody', 'Consumable'
    StockLevel INT,
    ExpiryDate DATE
);

INSERT INTO BiotechDB.Bronze.Experiments VALUES
('EXP-2025-001', 'Dr. Smith', '2025-01-10', 'PROT-A', 'Completed'),
('EXP-2025-002', 'Dr. Jones', '2025-01-12', 'PROT-B', 'Failed'),
('EXP-2025-003', 'Dr. Smith', '2025-01-15', 'PROT-A', 'In Progress');

INSERT INTO BiotechDB.Bronze.LabResults VALUES
(1, 'EXP-2025-001', 'CellCount', 1.5e6, '2025-01-11 10:00:00'),
(2, 'EXP-2025-001', 'Viability', 95.5, '2025-01-11 10:00:00'),
(3, 'EXP-2025-002', 'CellCount', 0.2e6, '2025-01-13 09:00:00'), -- Low count, likely failed
(4, 'EXP-2025-002', 'Viability', 40.0, '2025-01-13 09:00:00');

INSERT INTO BiotechDB.Bronze.Inventory VALUES
(101, 'Growth Media X', 'Reagent', 50, '2025-06-01'),
(102, 'Antibody Y', 'Antibody', 5, '2025-03-15'),
(103, 'Pipette Tips', 'Consumable', 200, '2030-01-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BiotechDB.Silver.ExperimentAnalysis AS
SELECT 
    e.ExpID,
    e.Researcher,
    e.ProtocolID,
    e.Status,
    AVG(CASE WHEN r.MetricName = 'Viability' THEN r.MetricValue ELSE NULL END) AS AvgViability,
    MAX(CASE WHEN r.MetricName = 'CellCount' THEN r.MetricValue ELSE NULL END) AS MaxCellCount
FROM BiotechDB.Bronze.Experiments e
LEFT JOIN BiotechDB.Bronze.LabResults r ON e.ExpID = r.ExpID
GROUP BY e.ExpID, e.Researcher, e.ProtocolID, e.Status;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BiotechDB.Gold.ProtocolSuccessRates AS
SELECT 
    ProtocolID,
    COUNT(ExpID) AS TotalExperiments,
    SUM(CASE WHEN Status = 'Completed' AND AvgViability > 90 THEN 1 ELSE 0 END) AS SuccessfulExperiments,
    (CAST(SUM(CASE WHEN Status = 'Completed' AND AvgViability > 90 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(ExpID)) * 100 AS SuccessRate
FROM BiotechDB.Silver.ExperimentAnalysis
GROUP BY ProtocolID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which ProtocolID has the highest SuccessRate in BiotechDB.Gold.ProtocolSuccessRates?"
*/
