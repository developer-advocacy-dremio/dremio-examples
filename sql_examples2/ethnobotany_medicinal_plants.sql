/*
 * Dremio Ethnobotany Medicinal Plants Example
 * 
 * Domain: Pharma Research & Anthropology
 * Scenario: 
 * Researchers catalog traditional plant usage by indigenous cultures. 
 * They then analyze chemical extracts from these plants in the lab.
 * The goal is to identify "Bio-Active Candidates" by correlating centuries of 
 * traditional knowledge with modern Mass Spectrometry data.
 * 
 * Complexity: Medium (Text correlation to chemical data, ranking logic)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Pharma_Bio;
CREATE FOLDER IF NOT EXISTS Pharma_Bio.Sources;
CREATE FOLDER IF NOT EXISTS Pharma_Bio.Bronze;
CREATE FOLDER IF NOT EXISTS Pharma_Bio.Silver;
CREATE FOLDER IF NOT EXISTS Pharma_Bio.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Pharma_Bio.Sources.Field_Notes (
    EntryID VARCHAR,
    Plant_Scientific_Name VARCHAR,
    Region VARCHAR, -- 'Amazon', 'Andes', 'Congo Basin'
    Traditional_Use VARCHAR, -- 'Fever', 'Pain', 'Infection', 'Ritual'
    Preparation_Method VARCHAR, -- 'Boiled Tea', 'Poultice', 'Raw'
    Healer_Confidence_Level INT -- 1-10 (How strongly the culture relies on it)
);

CREATE TABLE IF NOT EXISTS Pharma_Bio.Sources.Lab_Assays (
    AssayID VARCHAR,
    Plant_Ref_Name VARCHAR,
    Extract_Solvent VARCHAR, -- 'Ethanol', 'Water', 'Hexane'
    Active_Compound_Class VARCHAR, -- 'Alkaloid', 'Terpenoid', 'Flavonoid'
    Bio_Activity_Score DOUBLE, -- Inhibition % in petri dish
    Toxicity_Level INT -- 0=Safe, 10=Lethal
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Field Notes (Ethnobotany)
INSERT INTO Pharma_Bio.Sources.Field_Notes VALUES
('FN-001', 'Artemisia annua', 'Asia', 'Fever', 'Boiled Tea', 9),
('FN-002', 'Cinchona officinalis', 'Andes', 'Fever', 'Bark Chew', 10),
('FN-003', 'Papaver somniferum', 'Europe', 'Pain', 'Latex', 10),
('FN-004', 'Salix alba', 'Europe', 'Pain', 'Bark Tea', 8),
('FN-005', 'Cannabis sativa', 'Asia', 'Pain', 'Smoked', 9),
('FN-006', 'Curcuma longa', 'Asia', 'Inflammation', 'Powder', 7),
('FN-007', 'Panax ginseng', 'Asia', 'Energy', 'Root Decoction', 8),
('FN-008', 'Echinacea purpurea', 'North America', 'Infection', 'Root Chew', 6),
('FN-009', 'Uncaria tomentosa', 'Amazon', 'Inflammation', 'Bark Tea', 9),
('FN-010', 'Banisteriopsis caapi', 'Amazon', 'Ritual', 'Ayahuasca Brew', 10),
('FN-011', 'Psychotria viridis', 'Amazon', 'Ritual', 'Ayahuasca Brew', 10),
('FN-012', 'Aloe vera', 'Africa', 'Skin Burn', 'Gel', 9),
('FN-013', 'Azadirachta indica', 'India', 'Infection', 'Oil', 8),
('FN-014', 'Ocimum sanctum', 'India', 'Cough', 'Leaf Tea', 7),
('FN-015', 'Taxus brevifolia', 'North America', 'None (Toxic)', 'None', 0), -- Pacific Yew (Source of Taxol)
('FN-016', 'Catharanthus roseus', 'Madagascar', 'Diabetes', 'Leaf Tea', 6),
('FN-017', 'Digitalis purpurea', 'Europe', 'Heart', 'Controlled Dose', 9), -- Foxglove
('FN-018', 'Atropa belladonna', 'Europe', 'Ritual', 'Ointment', 9), -- Deadly Nightshade
('FN-019', 'Rauvolfia serpentina', 'India', 'Madness', 'Root', 8),
('FN-020', 'Ephedra sinica', 'China', 'Asthma', 'Tea', 8),
('FN-021', 'Camellia sinensis', 'Asia', 'Focus', 'Tea', 10),
('FN-022', 'Coffea arabica', 'Africa', 'Energy', 'Bean Brew', 10),
('FN-023', 'Theobroma cacao', 'Amazon', 'Energy', 'Bean Paste', 9),
('FN-024', 'Erythroxylum coca', 'Andes', 'Energy', 'Leaf Chew', 10),
('FN-025', 'Nicotiana tabacum', 'Americas', 'Ritual', 'Smoked', 10),
('FN-026', 'Valeriana officinalis', 'Europe', 'Sleep', 'Root Tea', 7),
('FN-027', 'Hypericum perforatum', 'Europe', 'Mood', 'Flower Tea', 6),
('FN-028', 'Matricaria chamomilla', 'Europe', 'Sleep', 'Flower Tea', 8),
('FN-029', 'Mentha piperita', 'Europe', 'Digestion', 'Leaf Tea', 8),
('FN-030', 'Zingiber officinale', 'Asia', 'Nausea', 'Root Tea', 9),
('FN-031', 'Ginkgo biloba', 'Asia', 'Memory', 'Leaf Extract', 5),
('FN-032', 'Allium sativum', 'Asia', 'Infection', 'Raw Bulb', 7),
('FN-033', 'Piper methysticum', 'Pacific', 'Relaxation', 'Root Brew', 9), -- Kava
('FN-034', 'Mitragyna speciosa', 'Southeast Asia', 'Pain', 'Leaf Chew', 8), -- Kratom
('FN-035', 'Tabernanthe iboga', 'Africa', 'Ritual', 'Root Bark', 10),
('FN-036', 'Lophophora williamsii', 'Mexico', 'Ritual', 'Button', 10), -- Peyote
('FN-037', 'Trichocereus pachanoi', 'Andes', 'Ritual', 'Brew', 10), -- San Pedro
('FN-038', 'Salvia divinorum', 'Mexico', 'Ritual', 'Leaf', 9),
('FN-039', 'Psilocybe cubensis', 'Global', 'Ritual', 'Fungus', 10),
('FN-040', 'Amanita muscaria', 'Siberia', 'Ritual', 'Fungus', 8),
('FN-041', 'Datura stramonium', 'Americas', 'Ritual', 'Seed', 9), -- Dangerous
('FN-042', 'Brugmansia arborea', 'Andes', 'Ritual', 'Flower Tea', 9),
('FN-043', 'Passiflora incarnata', 'Americas', 'Sleep', 'Vine Tea', 7),
('FN-044', 'Humulus lupulus', 'Europe', 'Sleep', 'Flower', 6),
('FN-045', 'Lavandula angustifolia', 'Europe', 'Sleep', 'Oil', 7),
('FN-046', 'Rosmarinus officinalis', 'Europe', 'Memory', 'Leaf', 5),
('FN-047', 'Thymus vulgaris', 'Europe', 'Cough', 'Leaf', 6),
('FN-048', 'Origanum vulgare', 'Europe', 'Infection', 'Oil', 7),
('FN-049', 'Sambucus nigra', 'Europe', 'Flu', 'Berry', 8),
('FN-050', 'Eucalyptus globulus', 'Australia', 'Congestion', 'Leaf Steam', 9);


-- Seed Lab Results (Correlating or contradicting)
INSERT INTO Pharma_Bio.Sources.Lab_Assays VALUES
('LAB-001', 'Artemisia annua', 'Ethanol', 'Terpenoid', 95.0, 1), -- Artemisinin (Malaria), Very Active
('LAB-002', 'Cinchona officinalis', 'Ethanol', 'Alkaloid', 92.0, 2), -- Quinine
('LAB-003', 'Papaver somniferum', 'Ethanol', 'Alkaloid', 99.0, 8), -- Morphine (High toxicity risk)
('LAB-004', 'Salix alba', 'Water', 'Phenolic', 40.0, 1), -- Salicylic acid (Aspirin precursor)
('LAB-005', 'Taxus brevifolia', 'Ethanol', 'Terpenoid', 88.0, 7), -- Taxol (Cancer)
('LAB-006', 'Catharanthus roseus', 'Ethanol', 'Alkaloid', 85.0, 6), -- Vinblastine
('LAB-007', 'Digitalis purpurea', 'Water', 'Glycoside', 90.0, 9), -- Digoxin (High Toxicity)
('LAB-008', 'Curcuma longa', 'Ethanol', 'Phenolic', 20.0, 0), -- Curcumin (Low bioavailability)
('LAB-009', 'Curcuma longa', 'Lipid', 'Phenolic', 60.0, 0), -- Better with fat
('LAB-010', 'Echinacea purpurea', 'Water', 'Polysaccharide', 15.0, 0), -- Low activity in lab?
('LAB-011', 'Uncaria tomentosa', 'Ethanol', 'Alkaloid', 45.0, 1),
('LAB-012', 'Banisteriopsis caapi', 'Water', 'Alkaloid', 70.0, 3), -- MAOI
('LAB-013', 'Psychotria viridis', 'Water', 'Alkaloid', 0.0, 1), -- Inactive orally without MAOI
('LAB-014', 'Psychotria viridis', 'Ethanol', 'Alkaloid', 80.0, 1), -- DMT
('LAB-015', 'Atropa belladonna', 'Ethanol', 'Alkaloid', 95.0, 9), -- Atropine
('LAB-016', 'Rauvolfia serpentina', 'Ethanol', 'Alkaloid', 85.0, 5), -- Reserpine
('LAB-017', 'Ephedra sinica', 'Water', 'Alkaloid', 88.0, 4), -- Ephedrine
('LAB-018', 'Datura stramonium', 'Ethanol', 'Alkaloid', 90.0, 9), -- Scopolamine
('LAB-019', 'Passiflora incarnata', 'Ethanol', 'Flavonoid', 30.0, 0),
('LAB-020', 'Valeriana officinalis', 'Ethanol', 'Terpenoid', 40.0, 1),
('LAB-021', 'Hypericum perforatum', 'Ethanol', 'Anthraquinone', 60.0, 2), -- St Johns Wort
('LAB-022', 'Panax ginseng', 'Water', 'Saponin', 35.0, 0),
('LAB-023', 'Ginkgo biloba', 'Ethanol', 'Flavonoid', 25.0, 0),
('LAB-024', 'Allium sativum', 'Raw', 'Sulfur', 50.0, 1), -- Allicin
('LAB-025', 'Azadirachta indica', 'Oil', 'Terpenoid', 75.0, 2), -- Neem
('LAB-026', 'Ocimum sanctum', 'Water', 'Terpenoid', 30.0, 0),
('LAB-027', 'Eucalyptus globulus', 'Steam', 'Terpenoid', 65.0, 3), -- Eucalyptol
('LAB-028', 'Sambucus nigra', 'Water', 'Antioxidant', 40.0, 1),
('LAB-029', 'Thymus vulgaris', 'Oil', 'Phenolic', 70.0, 2), -- Thymol
('LAB-030', 'Origanum vulgare', 'Oil', 'Phenolic', 72.0, 2), -- Carvacrol
('LAB-031', 'Mentha piperita', 'Oil', 'Terpenoid', 50.0, 1), -- Menthol
('LAB-032', 'Zingiber officinale', 'Water', 'Phenolic', 35.0, 1),
('LAB-033', 'Piper methysticum', 'Water', 'Lactone', 60.0, 2), -- Kavalactones
('LAB-034', 'Mitragyna speciosa', 'Ethanol', 'Alkaloid', 75.0, 4), -- Mitragynine
('LAB-035', 'Tabernanthe iboga', 'Ethanol', 'Alkaloid', 90.0, 6), -- Ibogaine
('LAB-036', 'Trichocereus pachanoi', 'Water', 'Alkaloid', 65.0, 2), -- Mescaline
('LAB-037', 'Erythroxylum coca', 'Saliva', 'Alkaloid', 40.0, 2), -- Mild simulation
('LAB-038', 'Erythroxylum coca', 'Ethanol', 'Alkaloid', 90.0, 5), -- Concentrated
('LAB-039', 'Coffea arabica', 'Water', 'Alkaloid', 85.0, 2), -- Caffeine
('LAB-040', 'Camellia sinensis', 'Water', 'Alkaloid', 60.0, 1),
('LAB-041', 'Theobroma cacao', 'Water', 'Alkaloid', 40.0, 1), -- Theobromine
('LAB-042', 'Unknown Weed', 'Ethanol', 'Unknown', 5.0, 0), -- Control
('LAB-043', 'Grass', 'Water', 'Unknown', 1.0, 0), -- Control
('LAB-044', 'Pine Bark', 'Ethanol', 'Phenolic', 30.0, 0),
('LAB-045', 'Oak Bark', 'Water', 'Tannin', 25.0, 1),
('LAB-046', 'Willow Bark', 'Ethanol', 'Phenolic', 55.0, 1), -- Better extraction
('LAB-047', 'Clove', 'Oil', 'Phenolic', 80.0, 3), -- Eugenol
('LAB-048', 'Cinnamon', 'Ethanol', 'Aldehyde', 45.0, 2),
('LAB-049', 'Nutmeg', 'Ethanol', 'Phenylpropene', 20.0, 4), -- Myristicin
('LAB-050', 'Saffron', 'Water', 'Carotenoid', 35.0, 0);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Pharma_Bio.Bronze.Bronze_Knowledge AS SELECT * FROM Pharma_Bio.Sources.Field_Notes;
CREATE OR REPLACE VIEW Pharma_Bio.Bronze.Bronze_Lab AS SELECT * FROM Pharma_Bio.Sources.Lab_Assays;

-- 4b. SILVER LAYER (Bioprospecting Correlation)
CREATE OR REPLACE VIEW Pharma_Bio.Silver.Silver_Bio_Prospecting AS
SELECT
    f.Scientific_Name, -- Renaming for clarity (using matched column names assumed)
    f.Region,
    f.Traditional_Use,
    f.Healer_Confidence_Level,
    l.Active_Compound_Class,
    l.Extract_Solvent,
    l.Bio_Activity_Score,
    l.Toxicity_Level,
    -- Discovery Potential Score
    -- High Confidence + High Lab Activity = High Potential
    (f.Healer_Confidence_Level * l.Bio_Activity_Score) / NULLIF(l.Toxicity_Level + 1, 0) as Discovery_Index
FROM Pharma_Bio.Bronze.Bronze_Knowledge f
LEFT JOIN Pharma_Bio.Bronze.Bronze_Lab l ON f.Plant_Scientific_Name = l.Plant_Ref_Name;

-- 4c. GOLD LAYER (Drug Candidates)
CREATE OR REPLACE VIEW Pharma_Bio.Gold.Gold_Top_Candidates AS
SELECT
    Scientific_Name,
    Traditional_Use,
    Active_Compound_Class,
    MAX(Discovery_Index) as Peak_Score,
    AVG(Bio_Activity_Score) as Avg_Activity,
    CASE 
        WHEN MAX(Discovery_Index) > 500 THEN 'Prioritize for Clinical Trials'
        WHEN MAX(Discovery_Index) > 200 THEN 'Further Lab Study'
        ELSE 'Archive'
    END as Recommendation
FROM Pharma_Bio.Silver.Silver_Bio_Prospecting
WHERE Toxicity_Level < 8 -- Exclude lethal poisons from medicine list
GROUP BY Scientific_Name, Traditional_Use, Active_Compound_Class
ORDER BY Peak_Score DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Silver_Bio_Prospecting' for matches where 'Healer_Confidence_Level' is high (>8) 
 * but 'Bio_Activity_Score' is low (<20) in 'Water' solvent. 
 * Suggest trying 'Ethanol' or 'Oil' extraction based on similar successful compounds."
 */
