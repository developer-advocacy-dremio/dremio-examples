/*
 * Manufacturing: Textile Fabric Sourcing Quality Control
 * 
 * Scenario:
 * Analyzing defect rates in fabric rolls from international suppliers to rate supplier performance.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS TextileDB;
CREATE FOLDER IF NOT EXISTS TextileDB.Sourcing;
CREATE FOLDER IF NOT EXISTS TextileDB.Sourcing.Bronze;
CREATE FOLDER IF NOT EXISTS TextileDB.Sourcing.Silver;
CREATE FOLDER IF NOT EXISTS TextileDB.Sourcing.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Import Data
-------------------------------------------------------------------------------

-- Suppliers Table
CREATE TABLE IF NOT EXISTS TextileDB.Sourcing.Bronze.Suppliers (
    SupplierID INT,
    SupplierName VARCHAR,
    Country VARCHAR,
    MaterialSpecialty VARCHAR -- Cotton, Silk, Polyester, Wool
);

INSERT INTO TextileDB.Sourcing.Bronze.Suppliers VALUES
(1, 'SilkRoad Fabrics', 'China', 'Silk'),
(2, 'Bengal Cottons', 'India', 'Cotton'),
(3, 'Vietnam Text', 'Vietnam', 'Polyester'),
(4, 'Anatolian Wool', 'Turkey', 'Wool'),
(5, 'Cairo Fine Threads', 'Egypt', 'Cotton'),
(6, 'Milan Synthetics', 'Italy', 'Polyester'),
(7, 'Kyoto Silks', 'Japan', 'Silk'),
(8, 'Mumbai Mills', 'India', 'Cotton'),
(9, 'Dhaka Weavers', 'Bangladesh', 'Cotton'),
(10, 'Istanbul Textiles', 'Turkey', 'Wool'),
(11, 'Shanghai Silks', 'China', 'Silk'),
(12, 'Seoul Fabrics', 'South Korea', 'Polyester'),
(13, 'Lima Wool', 'Peru', 'Wool'),
(14, 'Sydney Wool Co', 'Australia', 'Wool'),
(15, 'US Cotton Inc', 'USA', 'Cotton'),
(16, 'Synthetic Solutions', 'Germany', 'Polyester'),
(17, 'French Lace', 'France', 'Silk'),
(18, 'Thai Silks', 'Thailand', 'Silk'),
(19, 'IndoTech', 'Indonesia', 'Polyester'),
(20, 'Pakistan Cottons', 'Pakistan', 'Cotton'),
(21, 'Lagos Fabrics', 'Nigeria', 'Cotton'),
(22, 'Ethiopian Text', 'Ethiopia', 'Cotton'),
(23, 'Mexican Weaves', 'Mexico', 'Cotton'),
(24, 'Brazil Cottons', 'Brazil', 'Cotton'),
(25, 'Andean Alpacas', 'Peru', 'Wool'),
(26, 'NZ Merino', 'New Zealand', 'Wool'),
(27, 'Scottish Tweeds', 'UK', 'Wool'),
(28, 'Irish Linens', 'Ireland', 'Cotton'), -- Linen categorized as cotton-like for sim
(29, 'Taiwan Tech', 'Taiwan', 'Polyester'),
(30, 'Malaysia Poly', 'Malaysia', 'Polyester'),
(31, 'Portugal Fabrics', 'Portugal', 'Cotton'),
(32, 'Spain Textiles', 'Spain', 'Polyester'),
(33, 'Morocco Rugs', 'Morocco', 'Wool'),
(34, 'Tunisia Tech', 'Tunisia', 'Cotton'),
(35, 'Sri Lanka Garments', 'Sri Lanka', 'Cotton'),
(36, 'Cambodia Text', 'Cambodia', 'Polyester'),
(37, 'Laos Silks', 'Laos', 'Silk'),
(38, 'Suzhou Silks', 'China', 'Silk'),
(39, 'Gujarat Cottons', 'India', 'Cotton'),
(40, 'Bursa Fabrics', 'Turkey', 'Silk'),
(41, 'Lyon Silks', 'France', 'Silk'),
(42, 'Como Silks', 'Italy', 'Silk'),
(43, 'Prato Wool', 'Italy', 'Wool'),
(44, 'Yorkshire Wool', 'UK', 'Wool'),
(45, 'Carolina Cotton', 'USA', 'Cotton'),
(46, 'Texas Cotton', 'USA', 'Cotton'),
(47, 'Honduras Knits', 'Honduras', 'Cotton'),
(48, 'El Salvador Text', 'El Salvador', 'Polyester'),
(49, 'Colombia Fabrics', 'Colombia', 'Cotton'),
(50, 'Argentina Wool', 'Argentina', 'Wool');

-- FabricBatches Table: Inspection results per batch
CREATE TABLE IF NOT EXISTS TextileDB.Sourcing.Bronze.FabricBatches (
    BatchID VARCHAR,
    SupplierID INT,
    MaterialType VARCHAR,
    YardsReceived DOUBLE,
    DefectCount INT,
    InspectionDate DATE
);

INSERT INTO TextileDB.Sourcing.Bronze.FabricBatches VALUES
('B-1001', 1, 'Silk', 1000.0, 5, '2025-01-01'),
('B-1002', 2, 'Cotton', 5000.0, 50, '2025-01-02'), -- High defect
('B-1003', 3, 'Polyester', 2000.0, 2, '2025-01-03'),
('B-1004', 4, 'Wool', 500.0, 0, '2025-01-04'),
('B-1005', 5, 'Cotton', 3000.0, 10, '2025-01-05'),
('B-1006', 1, 'Silk', 1200.0, 8, '2025-01-06'),
('B-1007', 2, 'Cotton', 4500.0, 40, '2025-01-07'),
('B-1008', 6, 'Polyester', 1500.0, 15, '2025-01-08'),
('B-1009', 7, 'Silk', 800.0, 1, '2025-01-09'), -- High Quality
('B-1010', 8, 'Cotton', 6000.0, 120, '2025-01-10'), -- Very high defect
('B-1011', 9, 'Cotton', 5500.0, 60, '2025-01-11'),
('B-1012', 10, 'Wool', 600.0, 2, '2025-01-12'),
('B-1013', 11, 'Silk', 1100.0, 12, '2025-01-13'),
('B-1014', 12, 'Polyester', 2200.0, 5, '2025-01-14'),
('B-1015', 13, 'Wool', 700.0, 3, '2025-01-15'),
('B-1016', 14, 'Wool', 900.0, 1, '2025-01-16'), -- Good wool
('B-1017', 15, 'Cotton', 8000.0, 20, '2025-01-17'), -- Good cotton
('B-1018', 16, 'Polyester', 2500.0, 8, '2025-01-18'),
('B-1019', 17, 'Silk', 500.0, 0, '2025-01-19'),
('B-1020', 18, 'Silk', 600.0, 4, '2025-01-20'),
('B-1021', 19, 'Polyester', 3000.0, 30, '2025-01-21'),
('B-1022', 20, 'Cotton', 4000.0, 45, '2025-01-22'),
('B-1023', 21, 'Cotton', 3500.0, 35, '2025-01-23'),
('B-1024', 22, 'Cotton', 2500.0, 25, '2025-01-24'),
('B-1025', 23, 'Cotton', 2800.0, 20, '2025-01-25'),
('B-1026', 24, 'Cotton', 3200.0, 28, '2025-01-26'),
('B-1027', 25, 'Wool', 400.0, 2, '2025-01-27'),
('B-1028', 26, 'Wool', 1500.0, 5, '2025-01-28'),
('B-1029', 27, 'Wool', 800.0, 6, '2025-01-29'),
('B-1030', 28, 'Cotton', 1200.0, 8, '2025-01-30'),
('B-1031', 29, 'Polyester', 2000.0, 10, '2025-02-01'),
('B-1032', 30, 'Polyester', 1800.0, 18, '2025-02-02'),
('B-1033', 31, 'Cotton', 1500.0, 10, '2025-02-03'),
('B-1034', 32, 'Polyester', 2200.0, 12, '2025-02-04'),
('B-1035', 33, 'Wool', 600.0, 8, '2025-02-05'),
('B-1036', 34, 'Cotton', 1900.0, 20, '2025-02-06'),
('B-1037', 35, 'Cotton', 2100.0, 22, '2025-02-07'),
('B-1038', 36, 'Polyester', 2400.0, 24, '2025-02-08'),
('B-1039', 37, 'Silk', 700.0, 5, '2025-02-09'),
('B-1040', 38, 'Silk', 900.0, 6, '2025-02-10'),
('B-1041', 39, 'Cotton', 4000.0, 50, '2025-02-11'),
('B-1042', 40, 'Silk', 800.0, 7, '2025-02-12'),
('B-1043', 41, 'Silk', 600.0, 2, '2025-02-13'),
('B-1044', 42, 'Silk', 500.0, 1, '2025-02-14'),
('B-1045', 43, 'Wool', 1000.0, 8, '2025-02-15'),
('B-1046', 44, 'Wool', 1200.0, 9, '2025-02-16'),
('B-1047', 45, 'Cotton', 7500.0, 15, '2025-02-17'),
('B-1048', 46, 'Cotton', 8200.0, 18, '2025-02-18'),
('B-1049', 47, 'Cotton', 3000.0, 30, '2025-02-19'),
('B-1050', 5, 'Cotton', 2500.0, 8, '2025-02-20');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Defect Rate Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TextileDB.Sourcing.Silver.BatchQuality AS
SELECT 
    b.BatchID,
    s.SupplierName,
    s.Country,
    b.MaterialType,
    b.YardsReceived,
    b.DefectCount,
    (CAST(b.DefectCount AS DOUBLE) / b.YardsReceived) * 1000 AS DefectsPer1000Yards,
    b.InspectionDate
FROM TextileDB.Sourcing.Bronze.FabricBatches b
JOIN TextileDB.Sourcing.Bronze.Suppliers s ON b.SupplierID = s.SupplierID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Supplier Scorecard
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TextileDB.Sourcing.Gold.SupplierScorecard AS
SELECT 
    SupplierName,
    Country,
    COUNT(BatchID) AS TotalBatches,
    SUM(YardsReceived) AS TotalVolume,
    AVG(DefectsPer1000Yards) AS AvgDefectRate,
    CASE 
        WHEN AVG(DefectsPer1000Yards) < 2 THEN 'A - Excellent'
        WHEN AVG(DefectsPer1000Yards) < 10 THEN 'B - Good'
        WHEN AVG(DefectsPer1000Yards) < 20 THEN 'C - Acceptable'
        ELSE 'D - Poor'
    END AS SupplierGrade
FROM TextileDB.Sourcing.Silver.BatchQuality
GROUP BY SupplierName, Country;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Find all suppliers with a Grade of 'A - Excellent' in the TextileDB.Sourcing.Gold.SupplierScorecard view."

PROMPT 2:
"Calculate the average defect rate per country using the Silver layer."

PROMPT 3:
"List all Fabric Batches where the DefectsPer1000Yards is greater than 15, ordered by inspection date."
*/
