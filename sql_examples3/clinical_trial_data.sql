/*
 * Dremio "Messy Data" Challenge: Clinical Trial Data
 * 
 * Scenario: 
 * Hospital trial logs.
 * 'Dosage' has mixed units ('500mg', '0.5g', '500').
 * 'Patient_ID' sometimes contains PII (Name) instead of ID.
 * 'Outcome' codes vary ('Recovered', 'Imp', 'No Change').
 * 
 * Objective for AI Agent:
 * 1. Anonymize PII: If Patient_ID looks like a name (contains space), hash it.
 * 2. Normalize Dosage: Convert all to 'mg'. (0.5g -> 500mg).
 * 3. Standardize Outcomes: Map to 'Positive', 'Neutral', 'Negative'.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Bio_Pharma;
CREATE FOLDER IF NOT EXISTS Bio_Pharma.Trials;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Bio_Pharma.Trials.DOSING_LOG (
    PATIENT_ID VARCHAR, -- 'P-001' or 'John Doe'
    DRUG_NAME VARCHAR,
    DOSAGE_RAW VARCHAR, -- '500mg', '0.5g'
    OUTCOME VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (PII, Units)
-------------------------------------------------------------------------------

-- Normal
INSERT INTO Bio_Pharma.Trials.DOSING_LOG VALUES
('P-001', 'DrugA', '500mg', 'Recovered'),
('P-002', 'DrugA', '250mg', 'Imp');

-- Unit Mix
INSERT INTO Bio_Pharma.Trials.DOSING_LOG VALUES
('P-003', 'DrugA', '0.5g', 'Recovered'), -- 500mg
('P-004', 'DrugA', '1g', 'Recovered'); -- 1000mg

-- Missing Units
INSERT INTO Bio_Pharma.Trials.DOSING_LOG VALUES
('P-005', 'DrugA', '500', 'No Change'); -- Assume mg?

-- PII Leak
INSERT INTO Bio_Pharma.Trials.DOSING_LOG VALUES
('John Smith', 'DrugB', '500mg', 'Side Effect'),
('Jane Doe', 'DrugB', '500mg', 'Side Effect');

-- Bulk Fill
INSERT INTO Bio_Pharma.Trials.DOSING_LOG VALUES
('P-006', 'DrugC', '100mg', 'Positive'),
('P-007', 'DrugC', '200mg', 'Negative'),
('P-008', 'DrugC', '300mg', 'Neutral'),
('P-009', 'DrugC', '5mg', 'Positive'),
('P-010', 'DrugC', '0.005g', 'Positive'), -- 5mg
('P-011', 'DrugC', '10kg', 'Death'), -- Error? 10kg is lethal
('P-012', 'DrugC', '-500mg', 'Error'),
('P-013', 'DrugC', NULL, 'Error'),
('P-014', 'DrugC', 'Unknown', 'Error'),
('P-015', 'DrugC', '500 ML', 'Error'), -- Liquid?
('P-016', 'DrugC', '500mg', 'Recovered'),
('P-017', 'DrugC', '500mg', 'Recovered'),
('P-018', 'DrugC', '500mg', 'Recovered'),
('P-019', 'DrugC', '500mg', 'Recovered'),
('P-020', 'DrugC', '500mg', 'Recovered'),
('P-021', 'DrugC', '500mg', 'Recovered'),
('P-022', 'DrugC', '500mg', 'Recovered'),
('P-023', 'DrugC', '500mg', 'Recovered'),
('P-024', 'DrugC', '500mg', 'Recovered'),
('P-025', 'DrugC', '500mg', 'Recovered'),
('P-026', 'DrugC', '500mg', 'Recovered'),
('P-027', 'DrugC', '500mg', 'Recovered'),
('P-028', 'DrugC', '500mg', 'Recovered'),
('P-029', 'DrugC', '500mg', 'Recovered'),
('P-030', 'DrugC', '500mg', 'Recovered'),
('Robert Paulson', 'DrugA', '500mg', 'N/A'),
('P-031', 'DrugA', '500mg', 'N/A'),
('P-032', 'DrugA', '500mg', 'N/A'),
('P-033', 'DrugA', '500mg', 'N/A'),
('P-034', 'DrugA', '500mg', 'N/A');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze trial results in Bio_Pharma.Trials.DOSING_LOG.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Anonymize: If Patient_ID contains space, assume PII and mask it.
 *     - Normalize Dosage: Parse numeric value and Convert 'g' to 1000 * 'mg'.
 *  3. Gold: 
 *     - Efficacy: Count 'Recovered'/'Positive' outcomes per Drug per Normalized_Dosage.
 *  
 *  Show the SQL."
 */
