/*
 * Dremio Digital Archaeology Site Mapping Example
 * 
 * Domain: Archaeology & Anthropology
 * Scenario: 
 * An archaeological dig generates massive amounts of data from 3D scans, artifact logs, 
 * and stratigraphic (soil layer) analysis. 
 * Researchers need to "contextualize" individual artifacts (Pottery, Bones) by linking them 
 * to the specific soil layer they were found in. This helps date the objects relative to the 
 * "Start" and "End" depth of historical eras (Bronze Age, Iron Age, etc.).
 * 
 * Complexity: Medium (Range Joins on Depth, Categorical Logic)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Archaeology_Dig;
CREATE FOLDER IF NOT EXISTS Archaeology_Dig.Sources;
CREATE FOLDER IF NOT EXISTS Archaeology_Dig.Bronze;
CREATE FOLDER IF NOT EXISTS Archaeology_Dig.Silver;
CREATE FOLDER IF NOT EXISTS Archaeology_Dig.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Archaeology_Dig.Sources.Artifact_Registry (
    ArtifactID VARCHAR,
    Excavation_Unit VARCHAR,
    Depth_CM DOUBLE, -- Depth below datum
    Material_Type VARCHAR, -- Pottery, Bone, Metal, Lithic
    Description VARCHAR,
    FinderID VARCHAR,
    Date_Found DATE
);

CREATE TABLE IF NOT EXISTS Archaeology_Dig.Sources.Stratigraphy_Layers (
    LayerID VARCHAR,
    Excavation_Unit VARCHAR,
    Depth_Start_CM DOUBLE,
    Depth_End_CM DOUBLE,
    Soil_Type VARCHAR,
    Estimated_Era VARCHAR, -- 'Roman', 'Iron Age', 'Bronze Age', 'Neolithic'
    Carbon_Date_Confidence DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Stratigraphy (Context layers for Units A and B)
INSERT INTO Archaeology_Dig.Sources.Stratigraphy_Layers VALUES
('L-A1', 'Unit-A', 0.0, 50.0, 'Topsoil', 'Modern', 0.99),
('L-A2', 'Unit-A', 50.0, 150.0, 'Dark Earth', 'Medieval', 0.90),
('L-A3', 'Unit-A', 150.0, 300.0, 'Clay Fill', 'Roman', 0.85),
('L-A4', 'Unit-A', 300.0, 450.0, 'Sandy Loam', 'Iron Age', 0.80),
('L-A5', 'Unit-A', 450.0, 600.0, 'Gravel', 'Bronze Age', 0.75),
('L-B1', 'Unit-B', 0.0, 40.0, 'Topsoil', 'Modern', 0.99),
('L-B2', 'Unit-B', 40.0, 120.0, 'Rubble', 'Victorian', 0.95),
('L-B3', 'Unit-B', 120.0, 250.0, 'Dark Earth', 'Medieval', 0.90),
('L-B4', 'Unit-B', 250.0, 400.0, 'Clay', 'Roman', 0.85),
('L-B5', 'Unit-B', 400.0, 550.0, 'Peat', 'Iron Age', 0.80),
('L-C1', 'Unit-C', 0.0, 60.0, 'Topsoil', 'Modern', 0.99),
('L-C2', 'Unit-C', 60.0, 140.0, 'Silt', 'Medieval', 0.88),
('L-C3', 'Unit-C', 140.0, 280.0, 'Clay', 'Roman', 0.82),
('L-C4', 'Unit-C', 280.0, 500.0, 'Chalk', 'Neolithic', 0.70);

-- Seed Artifacts
INSERT INTO Archaeology_Dig.Sources.Artifact_Registry VALUES
('ART-001', 'Unit-A', 10.0, 'Plastic', 'Bottle Cap', 'FINDER-01', '2023-07-01'), -- Modern
('ART-002', 'Unit-A', 25.0, 'Metal', 'Coin (Penny)', 'FINDER-01', '2023-07-01'),
('ART-003', 'Unit-A', 60.0, 'Pottery', 'Green Glaze Sherd', 'FINDER-02', '2023-07-02'), -- Medieval
('ART-004', 'Unit-A', 100.0, 'Bone', 'Animal Rib', 'FINDER-02', '2023-07-02'),
('ART-005', 'Unit-A', 160.0, 'Pottery', 'Samian Ware', 'FINDER-01', '2023-07-03'), -- Roman
('ART-006', 'Unit-A', 180.0, 'Metal', 'Brooch', 'FINDER-03', '2023-07-03'),
('ART-007', 'Unit-A', 220.0, 'Coin', 'Sestertius', 'FINDER-03', '2023-07-04'),
('ART-008', 'Unit-A', 310.0, 'Pottery', 'Coarse Ware', 'FINDER-01', '2023-07-05'), -- Iron Age
('ART-009', 'Unit-A', 350.0, 'Bone', 'Human Tooth', 'FINDER-02', '2023-07-05'),
('ART-010', 'Unit-A', 460.0, 'Lithic', 'Flint Scraper', 'FINDER-01', '2023-07-06'), -- Bronze Age
('ART-011', 'Unit-B', 15.0, 'Glass', 'Modern Bottle', 'FINDER-04', '2023-07-01'),
('ART-012', 'Unit-B', 50.0, 'Pottery', 'Willow Pattern', 'FINDER-04', '2023-07-02'), -- Victorian
('ART-013', 'Unit-B', 80.0, 'Metal', 'Nail', 'FINDER-05', '2023-07-02'),
('ART-014', 'Unit-B', 130.0, 'Pottery', 'Jug Handle', 'FINDER-04', '2023-07-03'), -- Medieval
('ART-015', 'Unit-B', 260.0, 'Tile', 'Tegula Fragment', 'FINDER-05', '2023-07-04'), -- Roman
('ART-016', 'Unit-B', 270.0, 'Pottery', 'Grey Ware', 'FINDER-05', '2023-07-04'),
('ART-017', 'Unit-B', 410.0, 'Wood', 'Post Stake', 'FINDER-04', '2023-07-05'), -- Iron Age
('ART-018', 'Unit-C', 20.0, 'Metal', 'Soda Can', 'FINDER-06', '2023-07-01'),
('ART-019', 'Unit-C', 70.0, 'Pottery', 'Rim Sherd', 'FINDER-06', '2023-07-02'), -- Medieval
('ART-020', 'Unit-C', 150.0, 'Coin', 'Denarius', 'FINDER-07', '2023-07-03'), -- Roman context?
('ART-021', 'Unit-C', 300.0, 'Lithic', 'Arrowhead', 'FINDER-07', '2023-07-04'), -- Neolithic
('ART-022', 'Unit-C', 350.0, 'Bone', 'Antler Pick', 'FINDER-06', '2023-07-05'),
('ART-023', 'Unit-C', 400.0, 'Pottery', 'Grooved Ware', 'FINDER-07', '2023-07-06'),
('ART-024', 'Unit-A', 5.0,  'Plastic', 'Wrapper', 'FINDER-01', '2023-07-01'),
('ART-025', 'Unit-A', 550.0, 'Metal', 'Bronze Axe', 'FINDER-01', '2023-07-07'), -- Bronze Age
('ART-026', 'Unit-B', 300.0, 'Coin', 'Roman Coin', 'FINDER-05', '2023-07-04'),
('ART-027', 'Unit-B', 310.0, 'Pottery', 'Amphora', 'FINDER-05', '2023-07-04'),
('ART-028', 'Unit-A', 170.0, 'Bone', 'Cow Tibia', 'FINDER-03', '2023-07-03'),
('ART-029', 'Unit-A', 190.0, 'Glass', 'Bead', 'FINDER-02', '2023-07-03'),
('ART-030', 'Unit-C', 450.0, 'Lithic', 'Flint Core', 'FINDER-06', '2023-07-06'),
-- Anachronisms (Out of place)
('ART-031', 'Unit-C', 450.0, 'Plastic', 'Toy Soldier', 'FINDER-06', '2023-07-06'), -- Plastic in Neolithic?
('ART-032', 'Unit-A', 460.0, 'Coin', 'Euro Cent', 'FINDER-02', '2023-07-06'), -- Coin in Bronze Age?
('ART-033', 'Unit-B', 420.0, 'Metal', 'Aluminum Foil', 'FINDER-04', '2023-07-05'), -- Foil in Iron Age?
-- Filling out to 50+
('ART-034', 'Unit-A', 155.0, 'Pottery', 'Sherd', 'FINDER-01', '2023-07-03'),
('ART-035', 'Unit-A', 158.0, 'Pottery', 'Sherd', 'FINDER-01', '2023-07-03'),
('ART-036', 'Unit-A', 200.0, 'Pottery', 'Sherd', 'FINDER-03', '2023-07-04'),
('ART-037', 'Unit-A', 205.0, 'Bone', 'Fragment', 'FINDER-03', '2023-07-04'),
('ART-038', 'Unit-B', 265.0, 'Bone', 'Fragment', 'FINDER-05', '2023-07-04'),
('ART-039', 'Unit-B', 275.0, 'Bone', 'Fragment', 'FINDER-05', '2023-07-04'),
('ART-040', 'Unit-B', 415.0, 'Wood', 'Charcoal', 'FINDER-04', '2023-07-05'),
('ART-041', 'Unit-B', 425.0, 'Wood', 'Charcoal', 'FINDER-04', '2023-07-05'),
('ART-042', 'Unit-C', 310.0, 'Lithic', 'Flake', 'FINDER-07', '2023-07-04'),
('ART-043', 'Unit-C', 320.0, 'Lithic', 'Flake', 'FINDER-07', '2023-07-04'),
('ART-044', 'Unit-C', 330.0, 'Lithic', 'Flake', 'FINDER-07', '2023-07-04'),
('ART-045', 'Unit-C', 360.0, 'Bone', 'Jawbone', 'FINDER-06', '2023-07-05'),
('ART-046', 'Unit-C', 370.0, 'Bone', 'Skull Frag', 'FINDER-06', '2023-07-05'),
('ART-047', 'Unit-A', 470.0, 'Lithic', 'Scraper', 'FINDER-01', '2023-07-06'),
('ART-048', 'Unit-A', 480.0, 'Lithic', 'Blade', 'FINDER-01', '2023-07-06'),
('ART-049', 'Unit-A', 12.0, 'Plastic', 'Pen Lid', 'FINDER-01', '2023-07-01'),
('ART-050', 'Unit-C', 480.0, 'Lithic', 'Axe Head', 'FINDER-06', '2023-07-06');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Archaeology_Dig.Bronze.Bronze_Artifacts AS
SELECT * FROM Archaeology_Dig.Sources.Artifact_Registry;

CREATE OR REPLACE VIEW Archaeology_Dig.Bronze.Bronze_Stratigraphy AS
SELECT * FROM Archaeology_Dig.Sources.Stratigraphy_Layers;

-- 4b. SILVER LAYER (Contextualization Join)
-- Join artifacts to the soil layer they were found in based on depth range
CREATE OR REPLACE VIEW Archaeology_Dig.Silver.Silver_Contextualized_Finds AS
SELECT
    a.ArtifactID,
    a.Description,
    a.Material_Type,
    a.Depth_CM,
    a.Excavation_Unit,
    s.LayerID,
    s.Soil_Type,
    s.Estimated_Era,
    s.Carbon_Date_Confidence,
    -- Flag if artifact seems anachronistic (e.g. Plastic in Roman layer)
    CASE 
        WHEN a.Material_Type = 'Plastic' AND s.Estimated_Era NOT IN ('Modern') THEN 'Anachronism Alert'
        WHEN a.Material_Type = 'Coin' AND s.Estimated_Era = 'Neolithic' THEN 'Anachronism Alert'
        WHEN a.Material_Type = 'Aluminum' AND s.Estimated_Era != 'Modern' THEN 'Anachronism Alert'
        ELSE 'Consistent'
    END AS Context_Flag
FROM Archaeology_Dig.Bronze.Bronze_Artifacts a
LEFT JOIN Archaeology_Dig.Bronze.Bronze_Stratigraphy s
  ON a.Excavation_Unit = s.Excavation_Unit
  AND a.Depth_CM >= s.Depth_Start_CM
  AND a.Depth_CM < s.Depth_End_CM;

-- 4c. GOLD LAYER
CREATE OR REPLACE VIEW Archaeology_Dig.Gold.Gold_Era_Distribution AS
SELECT
    Estimated_Era,
    COUNT(*) as Find_Count,
    -- Calculate percentage of total finds
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as Pct_Total_Finds,
    LISTAGG(DISTINCT Material_Type, ', ') as Materials_Found
FROM Archaeology_Dig.Silver.Silver_Contextualized_Finds
WHERE Estimated_Era IS NOT NULL
GROUP BY Estimated_Era;

CREATE OR REPLACE VIEW Archaeology_Dig.Gold.Gold_Unit_Density_Heatmap AS
SELECT
    Excavation_Unit,
    Estimated_Era,
    COUNT(*) as Artifact_Count
FROM Archaeology_Dig.Silver.Silver_Contextualized_Finds
GROUP BY Excavation_Unit, Estimated_Era;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Scan 'Silver_Contextualized_Finds' for key 'Anachronism Alert' records. 
 * If a 'Plastic' item is found continuously in 'Neolithic' layers (Unit C), check if the stratum was disturbed (e.g., animal burrowing)."
 */
