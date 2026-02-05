/*
 * Dremio Paleontology Stratigraphy Example
 * 
 * Domain: Earth Science & Biology
 * Scenario: 
 * Paleontologists document fossils with X/Y/Z coordinates in a Grid.
 * They track "Stratigraphic Layer" (Rock type) which dates the find.
 * The goal is to reconstruct the "Faunal Assemblage" (what lived together).
 * 
 * Complexity: Medium (3D spatial grouping, temporal classification)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Dino_Dig;
CREATE FOLDER IF NOT EXISTS Dino_Dig.Sources;
CREATE FOLDER IF NOT EXISTS Dino_Dig.Bronze;
CREATE FOLDER IF NOT EXISTS Dino_Dig.Bronze;
CREATE FOLDER IF NOT EXISTS Dino_Dig.Silver;
CREATE FOLDER IF NOT EXISTS Dino_Dig.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Dino_Dig.Sources.Site_Grid (
    Grid_Square VARCHAR, -- 'A1', 'A2'
    Sector_Name VARCHAR, -- 'Quarry_East'
    Supervisor VARCHAR
);

CREATE TABLE IF NOT EXISTS Dino_Dig.Sources.Fossil_Log (
    FindID VARCHAR,
    Grid_Square VARCHAR,
    Discovery_Date DATE,
    Species_Name VARCHAR, -- 'T-Rex', 'Triceratops'
    Bone_Type VARCHAR, -- 'Femur', 'Skull', 'Vertebra'
    Depth_Z_Meters DOUBLE, -- Depth from surface
    Rock_Layer VARCHAR -- 'Sandstone', 'Shale', 'Mudstone'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Grid
INSERT INTO Dino_Dig.Sources.Site_Grid VALUES
('A1', 'Quarry_East', 'Dr. Grant'),
('A2', 'Quarry_East', 'Dr. Grant'),
('B1', 'Quarry_West', 'Dr. Sattler');

-- Seed Fossils
-- Sandstone = Cretaceous (66MYA). Shale = Jurassic (150MYA).
INSERT INTO Dino_Dig.Sources.Fossil_Log VALUES
('F-001', 'A1', '2023-07-01', 'Tyrannosaurus', 'Tooth', 1.5, 'Sandstone'),
('F-002', 'A1', '2023-07-01', 'Tyrannosaurus', 'Jaw Fragment', 1.6, 'Sandstone'),
('F-003', 'A1', '2023-07-02', 'Triceratops', 'Femur', 1.5, 'Sandstone'),
('F-004', 'A2', '2023-07-01', 'Edmontosaurus', 'Rib', 1.4, 'Sandstone'),
('F-005', 'B1', '2023-07-01', 'Allosaurus', 'Claw', 4.0, 'Shale'), -- Deeper layer
('F-006', 'B1', '2023-07-02', 'Stegosaurus', 'Plate', 4.2, 'Shale'),
('F-007', 'A1', '2023-07-03', 'Triceratops', 'Horn Core', 1.5, 'Sandstone'),
('F-008', 'A1', '2023-07-04', 'Tyrannosaurus', 'Phalanx', 1.6, 'Sandstone'),
('F-009', 'A2', '2023-07-04', 'Edmontosaurus', 'Vertebra', 1.4, 'Sandstone'),
('F-010', 'B1', '2023-07-03', 'Allosaurus', 'Skull', 4.1, 'Shale'),
-- Fill 50
('F-011', 'A1', '2023-07-05', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-012', 'A1', '2023-07-06', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-013', 'A1', '2023-07-07', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-014', 'A1', '2023-07-08', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-015', 'A1', '2023-07-09', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-016', 'A1', '2023-07-10', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-017', 'A1', '2023-07-11', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-018', 'A1', '2023-07-12', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-019', 'A1', '2023-07-13', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-020', 'A1', '2023-07-14', 'Triceratops', 'Rib', 1.5, 'Sandstone'),
('F-021', 'A2', '2023-07-05', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-022', 'A2', '2023-07-06', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-023', 'A2', '2023-07-07', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-024', 'A2', '2023-07-08', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-025', 'A2', '2023-07-09', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-026', 'A2', '2023-07-10', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-027', 'A2', '2023-07-11', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-028', 'A2', '2023-07-12', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-029', 'A2', '2023-07-13', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-030', 'A2', '2023-07-14', 'Edmontosaurus', 'Femur', 1.4, 'Sandstone'),
('F-031', 'B1', '2023-07-05', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-032', 'B1', '2023-07-06', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-033', 'B1', '2023-07-07', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-034', 'B1', '2023-07-08', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-035', 'B1', '2023-07-09', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-036', 'B1', '2023-07-10', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-037', 'B1', '2023-07-11', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-038', 'B1', '2023-07-12', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-039', 'B1', '2023-07-13', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-040', 'B1', '2023-07-14', 'Allosaurus', 'Toe', 4.0, 'Shale'),
('F-041', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-042', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-043', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-044', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-045', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-046', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-047', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-048', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-049', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone'),
('F-050', 'A1', '2023-07-15', 'Unknown', 'Fragment', 1.5, 'Sandstone');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Dino_Dig.Bronze.Bronze_Grid AS SELECT * FROM Dino_Dig.Sources.Site_Grid;
CREATE OR REPLACE VIEW Dino_Dig.Bronze.Bronze_Fossils AS SELECT * FROM Dino_Dig.Sources.Fossil_Log;

-- 4b. SILVER LAYER (Temporal Classification)
CREATE OR REPLACE VIEW Dino_Dig.Silver.Silver_Strata_Age AS
SELECT
    f.FindID,
    f.Grid_Square,
    f.Species_Name,
    f.Bone_Type,
    f.Rock_Layer,
    f.Depth_Z_Meters,
    -- Assign Geological Era based on rock layer
    CASE 
        WHEN f.Rock_Layer = 'Sandstone' THEN 'Cretaceous (66-145 MYA)'
        WHEN f.Rock_Layer = 'Shale' THEN 'Jurassic (145-201 MYA)'
        WHEN f.Rock_Layer = 'Mudstone' THEN 'Triassic (201-252 MYA)'
        ELSE 'Unknown'
    END as Geological_Era
FROM Dino_Dig.Bronze.Bronze_Fossils f;

-- 4c. GOLD LAYER (Assemblage Summary)
CREATE OR REPLACE VIEW Dino_Dig.Gold.Gold_Faunal_Assemblage AS
SELECT
    Geological_Era,
    Species_Name,
    COUNT(*) as Specimen_Count,
    -- Body part distribution
    LISTAGG(DISTINCT Bone_Type, ', ') as Found_Parts
FROM Dino_Dig.Silver.Silver_Strata_Age
GROUP BY Geological_Era, Species_Name;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Review 'Gold_Faunal_Assemblage'. 
 * If a 'Tyrannosaurus' is found in the 'Jurassic' era, flag it as an anomaly (Time traveler or misidentification)."
 */
