/*
    Dremio High-Volume SQL Pattern: Supply Chain Packaging Waste
    
    Business Scenario:
    CPG companies must track packaging materials (Plastic, Paper, Glass) per SKU.
    Regulatory pressure requires reducing non-recyclable waste.
    
    Data Story:
    - Bronze: PackagingSpecs (Material wts per SKU), ShipmentHistory.
    - Silver: TotalWasteGenerated (Weight * Units Shipped).
    - Gold: SustainabilityReport (Recyclable vs Landfill tons).
    
    Medallion Architecture:
    - Bronze: PackagingSpecs, ShipmentHistory.
      *Volume*: 50+ records.
    - Silver: TotalWasteGenerated.
    - Gold: SustainabilityReport.
    
    Key Dremio Features:
    - SUM(Expression)
    - Grouping by Material Class
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SupplyChainDB;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Bronze;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Silver;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Gold;
USE SupplyChainDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SupplyChainDB.Bronze.PackagingSpecs (
    SKU STRING,
    MaterialType STRING, -- Plastic_PET, Cardboard, Styrofoam
    Weight_g DOUBLE,
    IsRecyclable BOOLEAN
);

INSERT INTO SupplyChainDB.Bronze.PackagingSpecs VALUES
('SKU-101', 'Cardboard', 50.0, TRUE), ('SKU-101', 'Plastic_Film', 5.0, FALSE),
('SKU-102', 'Glass', 200.0, TRUE), ('SKU-102', 'Paper_Label', 2.0, TRUE),
('SKU-103', 'Styrofoam', 30.0, FALSE), ('SKU-103', 'Cardboard', 100.0, TRUE),
('SKU-104', 'Plastic_HDPE', 40.0, TRUE),
('SKU-105', 'Plastic_MultiLayer', 10.0, FALSE);

CREATE OR REPLACE TABLE SupplyChainDB.Bronze.ShipmentHistory (
    ShipmentID STRING,
    ShipDate DATE,
    SKU STRING,
    UnitsShipped INT
);

-- Generate High Volume Data
INSERT INTO SupplyChainDB.Bronze.ShipmentHistory VALUES
('SH-001', DATE '2025-01-01', 'SKU-101', 1000),
('SH-002', DATE '2025-01-01', 'SKU-102', 500),
('SH-003', DATE '2025-01-02', 'SKU-103', 200), -- Styrofoam heavy
('SH-004', DATE '2025-01-02', 'SKU-104', 1000),
('SH-005', DATE '2025-01-03', 'SKU-105', 5000), -- Lots of non-recyclable film
('SH-006', DATE '2025-01-03', 'SKU-101', 1200),
('SH-007', DATE '2025-01-04', 'SKU-101', 1100),
('SH-008', DATE '2025-01-04', 'SKU-102', 600),
('SH-009', DATE '2025-01-05', 'SKU-103', 250),
('SH-010', DATE '2025-01-05', 'SKU-104', 1500),
('SH-011', DATE '2025-01-06', 'SKU-105', 4000),
('SH-012', DATE '2025-01-06', 'SKU-101', 1000),
('SH-013', DATE '2025-01-07', 'SKU-102', 400),
('SH-014', DATE '2025-01-07', 'SKU-103', 300),
('SH-015', DATE '2025-01-08', 'SKU-104', 1200),
('SH-016', DATE '2025-01-08', 'SKU-105', 6000),
('SH-017', DATE '2025-01-09', 'SKU-101', 1000),
('SH-018', DATE '2025-01-09', 'SKU-102', 550),
('SH-019', DATE '2025-01-10', 'SKU-103', 200),
('SH-020', DATE '2025-01-10', 'SKU-104', 1100),
('SH-021', DATE '2025-01-11', 'SKU-105', 5500),
('SH-022', DATE '2025-01-11', 'SKU-101', 1300),
('SH-023', DATE '2025-01-12', 'SKU-102', 700),
('SH-024', DATE '2025-01-12', 'SKU-103', 150),
('SH-025', DATE '2025-01-13', 'SKU-104', 1300),
('SH-026', DATE '2025-01-13', 'SKU-105', 4500),
('SH-027', DATE '2025-01-14', 'SKU-101', 900),
('SH-028', DATE '2025-01-14', 'SKU-102', 500),
('SH-029', DATE '2025-01-15', 'SKU-103', 300),
('SH-030', DATE '2025-01-15', 'SKU-104', 1400),
('SH-031', DATE '2025-01-16', 'SKU-105', 5000),
('SH-032', DATE '2025-01-16', 'SKU-101', 1200),
('SH-033', DATE '2025-01-17', 'SKU-102', 600),
('SH-034', DATE '2025-01-17', 'SKU-103', 200),
('SH-035', DATE '2025-01-18', 'SKU-104', 1000),
('SH-036', DATE '2025-01-18', 'SKU-105', 6000),
('SH-037', DATE '2025-01-19', 'SKU-101', 1100),
('SH-038', DATE '2025-01-19', 'SKU-102', 500),
('SH-039', DATE '2025-01-20', 'SKU-103', 250),
('SH-040', DATE '2025-01-20', 'SKU-104', 1500),
('SH-041', DATE '2025-01-21', 'SKU-105', 5500),
('SH-042', DATE '2025-01-21', 'SKU-101', 1000),
('SH-043', DATE '2025-01-22', 'SKU-102', 600),
('SH-044', DATE '2025-01-22', 'SKU-103', 150),
('SH-045', DATE '2025-01-23', 'SKU-104', 1200),
('SH-046', DATE '2025-01-23', 'SKU-105', 4000),
('SH-047', DATE '2025-01-24', 'SKU-101', 1300),
('SH-048', DATE '2025-01-24', 'SKU-102', 700),
('SH-049', DATE '2025-01-25', 'SKU-103', 200),
('SH-050', DATE '2025-01-25', 'SKU-104', 1100);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Total Waste Calculations
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SupplyChainDB.Silver.TotalWasteGenerated AS
SELECT
    s.ShipDate,
    p.MaterialType,
    p.IsRecyclable,
    SUM(s.UnitsShipped * p.Weight_g) / 1000000.0 AS TotalWeight_MetricTons
FROM SupplyChainDB.Bronze.ShipmentHistory s
JOIN SupplyChainDB.Bronze.PackagingSpecs p ON s.SKU = p.SKU
GROUP BY s.ShipDate, p.MaterialType, p.IsRecyclable;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Sustainability Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SupplyChainDB.Gold.SustainabilityReport AS
SELECT
    MaterialType,
    SUM(TotalWeight_MetricTons) AS TotalTonnage,
    CASE WHEN IsRecyclable THEN 'Diverted' ELSE 'Landfill' END AS Fate
FROM SupplyChainDB.Silver.TotalWasteGenerated
GROUP BY MaterialType, IsRecyclable;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Calculate total tonnage destined for Landfill."
    2. "Which Material Type generates the most waste?"
    3. "Show trend of daily packaging weight shipped."
*/
