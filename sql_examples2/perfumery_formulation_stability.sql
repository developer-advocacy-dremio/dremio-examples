/*
 * Dremio Perfumery Formulation Stability Example
 * 
 * Domain: Chemical Manufacturing (Fragrance)
 * Scenario: 
 * A Perfume house creates "Juice" (concentrate) using a pyramid of Top, Middle, and Base notes.
 * Formulations must pass "Stability Tests" (UV light, Heat, Cold) to ensure they don't separate or change smell.
 * The system tracks ingredient volatility and test outcomes to flag unstable formulas before mass production.
 * 
 * Complexity: Medium (Recursive BOM-like structure, Pass/Fail logic)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Fragrance_Lab;
CREATE FOLDER IF NOT EXISTS Fragrance_Lab.Sources;
CREATE FOLDER IF NOT EXISTS Fragrance_Lab.Bronze;
CREATE FOLDER IF NOT EXISTS Fragrance_Lab.Silver;
CREATE FOLDER IF NOT EXISTS Fragrance_Lab.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Fragrance_Lab.Sources.Formulas (
    FormulaID VARCHAR,
    Formula_Name VARCHAR, -- 'Midnight Rose', 'Ocean Breeze'
    Version INT,
    Creation_Date DATE,
    Perfumer VARCHAR
);

CREATE TABLE IF NOT EXISTS Fragrance_Lab.Sources.Ingredients (
    IngredientID VARCHAR,
    FormulaID VARCHAR,
    Ingredient_Name VARCHAR,
    Note_Tier VARCHAR, -- 'Top', 'Middle', 'Base'
    Percentage DOUBLE, -- Sum to 100% hopefully
    Is_Natural BOOLEAN
);

CREATE TABLE IF NOT EXISTS Fragrance_Lab.Sources.Stability_Tests (
    TestID VARCHAR,
    FormulaID VARCHAR,
    Test_Type VARCHAR, -- 'UV Limit', 'Heat 40C', 'Cold 4C'
    Duration_Weeks INT,
    Result_Outcome VARCHAR, -- 'Pass', 'Fail-Separation', 'Fail-Discoloration', 'Fail-OdorChange'
    Comments VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Formulas
INSERT INTO Fragrance_Lab.Sources.Formulas VALUES
('FORM-001', 'Midnight Rose', 1, '2023-01-01', 'Jean-Claude'),
('FORM-002', 'Ocean Breeze', 1, '2023-01-05', 'Sophia'),
('FORM-003', 'Citrus Blast', 1, '2023-01-10', 'Jean-Claude'),
('FORM-004', 'Oud Royale', 1, '2023-01-15', 'Ahmed'),
('FORM-005', 'Vanilla Sky', 1, '2023-02-01', 'Sophia');

-- Seed Ingredients (Simplified BOM)
INSERT INTO Fragrance_Lab.Sources.Ingredients VALUES
-- Midnight Rose
('ING-101', 'FORM-001', 'Bergamot Oil', 'Top', 10.0, true),
('ING-102', 'FORM-001', 'Rose Absolute', 'Middle', 20.0, true),
('ING-103', 'FORM-001', 'Jasmine Sambac', 'Middle', 10.0, true),
('ING-104', 'FORM-001', 'Patchouli', 'Base', 30.0, true),
('ING-105', 'FORM-001', 'Vanillin', 'Base', 25.0, false),
('ING-106', 'FORM-001', 'Musk Ketone', 'Base', 5.0, false),
-- Ocean Breeze
('ING-201', 'FORM-002', 'Lemon Oil', 'Top', 20.0, true),
('ING-202', 'FORM-002', 'Calone', 'Middle', 10.0, false), -- Marine scent
('ING-203', 'FORM-002', 'Lily of Valley', 'Middle', 10.0, false),
('ING-204', 'FORM-002', 'White Musk', 'Base', 60.0, false),
-- Citrus Blast
('ING-301', 'FORM-003', 'Orange Oil', 'Top', 40.0, true),
('ING-302', 'FORM-003', 'Lime', 'Top', 20.0, true),
('ING-303', 'FORM-003', 'Neroli', 'Middle', 10.0, true),
('ING-304', 'FORM-003', 'Cedarwood', 'Base', 30.0, true),
-- Oud Royale
('ING-401', 'FORM-004', 'Saffron', 'Top', 5.0, true),
('ING-402', 'FORM-004', 'Rose Otto', 'Middle', 10.0, true),
('ING-403', 'FORM-004', 'Oud (Agarwood)', 'Base', 50.0, true),
('ING-404', 'FORM-004', 'Ambergris', 'Base', 15.0, false), -- Synthetic sub usually
('ING-405', 'FORM-004', 'Sandalwood', 'Base', 20.0, true),
-- Vanilla Sky (Unstable?)
('ING-501', 'FORM-005', 'Almond', 'Top', 10.0, false),
('ING-502', 'FORM-005', 'Heliotrope', 'Middle', 20.0, false),
('ING-503', 'FORM-005', 'Vanilla Absolute', 'Base', 60.0, true), -- Risky for discoloration
('ING-504', 'FORM-005', 'Benzoin', 'Base', 10.0, true);

-- Seed Stability Tests
INSERT INTO Fragrance_Lab.Sources.Stability_Tests VALUES
('TEST-001', 'FORM-001', 'UV Limit', 4, 'Pass', 'No change'),
('TEST-002', 'FORM-001', 'Heat 40C', 4, 'Pass', 'Slight darkening, acceptable'),
('TEST-003', 'FORM-001', 'Cold 4C', 4, 'Pass', 'Clear'),
('TEST-004', 'FORM-002', 'UV Limit', 4, 'Pass', 'Stable'),
('TEST-005', 'FORM-002', 'Heat 40C', 4, 'Fail-OdorChange', 'Smells metallic'),
('TEST-006', 'FORM-002', 'Cold 4C', 4, 'Pass', 'Good'),
('TEST-007', 'FORM-003', 'UV Limit', 8, 'Fail-Discoloration', 'Turned brown (Citrus oxidation)'),
('TEST-008', 'FORM-003', 'Heat 40C', 8, 'Fail-OdorChange', 'Terpenes degraded'),
('TEST-009', 'FORM-004', 'Heat 40C', 12, 'Pass', 'Oud gets better with heat'),
('TEST-010', 'FORM-004', 'Cold 4C', 12, 'Fail-Separation', 'Resins crystallized'),
('TEST-011', 'FORM-005', 'UV Limit', 2, 'Fail-Discoloration', 'Vanilla turned dark brown instantly'),
('TEST-012', 'FORM-005', 'Heat 40C', 4, 'Pass', 'Stable smell'),
-- Re-tests on V2 (Simulated)
('TEST-013', 'FORM-003', 'UV Limit', 8, 'Pass', 'Added UV filter'),
('TEST-014', 'FORM-005', 'UV Limit', 4, 'Pass', 'Used synthetic vanillin, no browning'),
-- More tests
('TEST-015', 'FORM-001', 'Freezing', 1, 'Pass', 'No crystals'),
('TEST-016', 'FORM-002', 'Freezing', 1, 'Pass', 'No crystals'),
('TEST-017', 'FORM-004', 'Freezing', 1, 'Fail-Separation', 'Sediment observed'),
('TEST-018', 'FORM-001', 'Heat 50C', 1, 'Pass', 'Aggressive test passed'),
('TEST-019', 'FORM-002', 'Heat 50C', 1, 'Fail-OdorChange', 'Rancid'),
('TEST-020', 'FORM-003', 'Heat 50C', 1, 'Fail-OdorChange', 'Burnt rubber');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Fragrance_Lab.Bronze.Bronze_Formulas AS SELECT * FROM Fragrance_Lab.Sources.Formulas;
CREATE OR REPLACE VIEW Fragrance_Lab.Bronze.Bronze_Ingredients AS SELECT * FROM Fragrance_Lab.Sources.Ingredients;
CREATE OR REPLACE VIEW Fragrance_Lab.Bronze.Bronze_Tests AS SELECT * FROM Fragrance_Lab.Sources.Stability_Tests;

-- 4b. SILVER LAYER (Formula Composition)
CREATE OR REPLACE VIEW Fragrance_Lab.Silver.Silver_Formula_Breakdown AS
SELECT
    f.FormulaID,
    f.Formula_Name,
    f.Perfumer,
    i.Note_Tier,
    SUM(i.Percentage) as Tier_Percentage,
    LISTAGG(i.Ingredient_Name, ', ') as Ingredients_List
FROM Fragrance_Lab.Bronze.Bronze_Formulas f
JOIN Fragrance_Lab.Bronze.Bronze_Ingredients i ON f.FormulaID = i.FormulaID
GROUP BY f.FormulaID, f.Formula_Name, f.Perfumer, i.Note_Tier;

-- 4c. GOLD LAYER (Stability Report)
CREATE OR REPLACE VIEW Fragrance_Lab.Gold.Gold_Stability_Scorecard AS
SELECT
    f.Formula_Name,
    COUNT(t.TestID) as Tests_Run,
    SUM(CASE WHEN t.Result_Outcome = 'Pass' THEN 1 ELSE 0 END) as Tests_Passed,
    SUM(CASE WHEN t.Result_Outcome LIKE 'Fail%' THEN 1 ELSE 0 END) as Tests_Failed,
    CASE 
        WHEN SUM(CASE WHEN t.Result_Outcome LIKE 'Fail%' THEN 1 ELSE 0 END) > 0 THEN 'REJECTED'
        ELSE 'APPROVED'
    END as Final_Status,
    LISTAGG(DISTINCT t.Result_Outcome, ', ') as Failure_Modes
FROM Fragrance_Lab.Bronze.Bronze_Formulas f
JOIN Fragrance_Lab.Bronze.Bronze_Tests t ON f.FormulaID = t.FormulaID
GROUP BY f.Formula_Name;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Gold_Stability_Scorecard'. Identify which 'Perfumer' has the highest rejection rate 
 * due to 'Fail-Discoloration'. Link back to ingredients to see if 'Vanilla' is the common culprit."
 */
