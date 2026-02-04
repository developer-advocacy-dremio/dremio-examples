/*
 * Dremio Art Restoration Lab Example
 * 
 * Domain: Cultural Heritage & Chemistry
 * Scenario: 
 * Museum conservators restore damaged masterpieces.
 * They track "Solvent Usage" (Isopropanol, Acetone) to clean varnish, 
 * and "Pigment Analysis" (XRF scans) to identify original vs. later additions.
 * The goal is to track "Treatment Hours" and ensuring solvents don't damage original layers.
 * 
 * Complexity: Medium (Chemical compatibility logic, time tracking)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Museum_Conservation;
CREATE FOLDER IF NOT EXISTS Museum_Conservation.Sources;
CREATE FOLDER IF NOT EXISTS Museum_Conservation.Bronze;
CREATE FOLDER IF NOT EXISTS Museum_Conservation.Silver;
CREATE FOLDER IF NOT EXISTS Museum_Conservation.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Museum_Conservation.Sources.Artwork_Registry (
    ArtID VARCHAR,
    Title VARCHAR,
    Artist VARCHAR,
    Year_Created INT,
    Medium VARCHAR, -- 'Oil on Canvas', 'Tempera on Wood'
    Condition_Rating INT -- 1 (Poor) to 10 (Mint)
);

CREATE TABLE IF NOT EXISTS Museum_Conservation.Sources.Treatment_Logs (
    LogID VARCHAR,
    ArtID VARCHAR,
    Treatment_Date DATE,
    Conservator VARCHAR,
    Action_Type VARCHAR, -- 'Varnish Removal', 'Inpainting', 'Consolidation'
    Solvent_Used VARCHAR, -- 'Acetone', 'Water', 'Saliva', 'Iso-Octane'
    Hours_Spent DOUBLE,
    Outcome_Notes VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Artwork
INSERT INTO Museum_Conservation.Sources.Artwork_Registry VALUES
('ART-001', 'The Old Guitarist', 'Picasso', 1903, 'Oil on Canvas', 4),
('ART-002', 'Mona Lisa Copy', 'Da Vinci Studio', 1510, 'Oil on Poplar', 6),
('ART-003', 'Sunflowers', 'Van Gogh', 1888, 'Oil on Canvas', 5),
('ART-004', 'Water Lilies', 'Monet', 1919, 'Oil on Canvas', 7),
('ART-005', 'The Scream Lithograph', 'Munch', 1895, 'Lithograph', 3);

-- Seed Treatments
INSERT INTO Museum_Conservation.Sources.Treatment_Logs VALUES
('LOG-100', 'ART-001', '2023-01-01', 'Alice', 'Varnish Removal', 'Acetone', 2.5, 'Varnish yellowed, came off easily'),
('LOG-101', 'ART-001', '2023-01-02', 'Alice', 'Varnish Removal', 'Acetone', 3.0, 'Cleaning sky area'),
('LOG-102', 'ART-001', '2023-01-03', 'Alice', 'Consolidation', 'Fish Glue', 1.5, 'Flaking paint stabilized'),
('LOG-103', 'ART-001', '2023-01-04', 'Alice', 'Inpainting', 'None', 4.0, 'Retouching losses'),
('LOG-104', 'ART-002', '2023-02-01', 'Bob', 'Surface Clean', 'Saliva', 1.0, 'Enzymatic clean effective'),
('LOG-105', 'ART-002', '2023-02-02', 'Bob', 'Varnish Removal', 'Iso-Octane', 2.0, 'Slow solvent action'),
('LOG-106', 'ART-003', '2023-03-01', 'Charlie', 'Analysis', 'None', 0.5, 'XRF scan confirms Chrome Yellow'),
('LOG-107', 'ART-003', '2023-03-02', 'Charlie', 'Consolidation', 'Beva 371', 2.0, 'Lining canvas edges'),
('LOG-108', 'ART-004', '2023-04-01', 'Alice', 'Surface Clean', 'Water', 1.0, 'Removing dust'),
('LOG-109', 'ART-005', '2023-05-01', 'Diana', 'Bleaching', 'Light', 5.0, 'Reducing foxing stains'),
('LOG-110', 'ART-001', '2023-01-05', 'Alice', 'Varnish Application', 'Dammar', 1.0, 'Final coat'),
('LOG-111', 'ART-002', '2023-02-03', 'Bob', 'Inpainting', 'None', 3.0, 'Matching skin tones'),
('LOG-112', 'ART-002', '2023-02-04', 'Bob', 'Inpainting', 'None', 2.0, 'Matching background'),
('LOG-113', 'ART-003', '2023-03-03', 'Charlie', 'Varnish Removal', 'Ethanol', 1.0, 'Test patch'),
('LOG-114', 'ART-003', '2023-03-04', 'Charlie', 'Varnish Removal', 'Ethanol', 1.0, 'Stopped - pigment sensitive'), -- SENSITIVE!
('LOG-115', 'ART-005', '2023-05-02', 'Diana', 'Mending', 'Wheat Paste', 2.0, 'Tear repair'),
('LOG-116', 'ART-001', '2023-01-10', 'Alice', 'Documentation', 'None', 1.0, 'Photos taken'),
('LOG-117', 'ART-002', '2023-02-10', 'Bob', 'Documentation', 'None', 1.0, 'Photos taken'),
('LOG-118', 'ART-003', '2023-03-10', 'Charlie', 'Documentation', 'None', 1.0, 'Photos taken'),
('LOG-119', 'ART-004', '2023-04-10', 'Alice', 'Documentation', 'None', 1.0, 'Photos taken'),
('LOG-120', 'ART-005', '2023-05-10', 'Diana', 'Documentation', 'None', 1.0, 'Photos taken'),
-- Filling 50 rows
('LOG-121', 'ART-001', '2023-01-11', 'Alice', 'Check', 'None', 0.5, 'Stable'),
('LOG-122', 'ART-001', '2023-01-12', 'Alice', 'Check', 'None', 0.5, 'Stable'),
('LOG-123', 'ART-002', '2023-02-11', 'Bob', 'Check', 'None', 0.5, 'Stable'),
('LOG-124', 'ART-002', '2023-02-12', 'Bob', 'Check', 'None', 0.5, 'Stable'),
('LOG-125', 'ART-003', '2023-03-11', 'Charlie', 'Check', 'None', 0.5, 'Stable'),
('LOG-126', 'ART-003', '2023-03-12', 'Charlie', 'Check', 'None', 0.5, 'Stable'),
('LOG-127', 'ART-004', '2023-04-11', 'Alice', 'Check', 'None', 0.5, 'Stable'),
('LOG-128', 'ART-004', '2023-04-12', 'Alice', 'Check', 'None', 0.5, 'Stable'),
('LOG-129', 'ART-005', '2023-05-11', 'Diana', 'Check', 'None', 0.5, 'Stable'),
('LOG-130', 'ART-005', '2023-05-12', 'Diana', 'Check', 'None', 0.5, 'Stable'),
('LOG-131', 'ART-001', '2023-06-01', 'Alice', 'Monitor', 'None', 0.5, 'Humidity check'),
('LOG-132', 'ART-002', '2023-06-01', 'Bob', 'Monitor', 'None', 0.5, 'Humidity check'),
('LOG-133', 'ART-003', '2023-06-01', 'Charlie', 'Monitor', 'None', 0.5, 'Humidity check'),
('LOG-134', 'ART-004', '2023-06-01', 'Alice', 'Monitor', 'None', 0.5, 'Humidity check'),
('LOG-135', 'ART-005', '2023-06-01', 'Diana', 'Monitor', 'None', 0.5, 'Humidity check'),
('LOG-136', 'ART-001', '2023-07-01', 'Alice', 'Monitor', 'None', 0.5, 'Storage check'),
('LOG-137', 'ART-002', '2023-07-01', 'Bob', 'Monitor', 'None', 0.5, 'Storage check'),
('LOG-138', 'ART-003', '2023-07-01', 'Charlie', 'Monitor', 'None', 0.5, 'Storage check'),
('LOG-139', 'ART-004', '2023-07-01', 'Alice', 'Monitor', 'None', 0.5, 'Storage check'),
('LOG-140', 'ART-005', '2023-07-01', 'Diana', 'Monitor', 'None', 0.5, 'Storage check'),
('LOG-141', 'ART-001', '2023-08-01', 'Alice', 'Loan Prep', 'None', 2.0, 'Crating'),
('LOG-142', 'ART-001', '2023-08-02', 'Alice', 'Loan Prep', 'None', 1.0, 'Condition Report'),
('LOG-143', 'ART-004', '2023-08-01', 'Alice', 'Loan Prep', 'None', 2.0, 'Crating'),
('LOG-144', 'ART-004', '2023-08-02', 'Alice', 'Loan Prep', 'None', 1.0, 'Condition Report'),
('LOG-145', 'ART-002', '2023-09-01', 'Bob', 'Inpainting', 'None', 1.0, 'Touch up'),
('LOG-146', 'ART-003', '2023-09-01', 'Charlie', 'Inpainting', 'Qtips', 1.0, 'Touch up'),
('LOG-147', 'ART-005', '2023-09-01', 'Diana', 'Frame Repair', 'Glue', 3.0, 'Giled frame fix'),
('LOG-148', 'ART-005', '2023-09-02', 'Diana', 'Frame Repair', 'Gold Leaf', 2.0, 'Gilding'),
('LOG-149', 'ART-001', '2023-12-01', 'Alice', 'Return Check', 'None', 1.0, 'Returned from loan'),
('LOG-150', 'ART-004', '2023-12-01', 'Alice', 'Return Check', 'None', 1.0, 'Returned from loan');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Museum_Conservation.Bronze.Bronze_Registry AS SELECT * FROM Museum_Conservation.Sources.Artwork_Registry;
CREATE OR REPLACE VIEW Museum_Conservation.Bronze.Bronze_Treatments AS SELECT * FROM Museum_Conservation.Sources.Treatment_Logs;

-- 4b. SILVER LAYER (Project Tracking)
CREATE OR REPLACE VIEW Museum_Conservation.Silver.Silver_Project_Hours AS
SELECT
    t.ArtID,
    a.Title,
    a.Artist,
    t.Conservator,
    t.Action_Type,
    t.Solvent_Used,
    t.Hours_Spent,
    -- Cost Estimate (Internal billing usually $100/hr)
    t.Hours_Spent * 100 as Labor_Cost_USD,
    t.Outcome_Notes
FROM Museum_Conservation.Bronze.Bronze_Treatments t
JOIN Museum_Conservation.Bronze.Bronze_Registry a ON t.ArtID = a.ArtID;

-- 4c. GOLD LAYER (Conservation History)
CREATE OR REPLACE VIEW Museum_Conservation.Gold.Gold_Treatment_Summary AS
SELECT
    Title,
    Artist,
    SUM(Hours_Spent) as Total_Hours,
    COUNT(DISTINCT Action_Type) as Distinct_Interventions,
    LISTAGG(DISTINCT Solvent_Used, ', ') as Chemistry_Profile,
    MAX(Treatment_Date) as Last_Treated
FROM Museum_Conservation.Silver.Silver_Project_Hours
GROUP BY Title, Artist
ORDER BY Total_Hours DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Review 'Gold_Treatment_Summary' to identify artworks that required 'Ethanol' or 'Acetone'. 
 * Check the notes in Silver to see if any sensitivity or damage was reported with these solvents."
 */
