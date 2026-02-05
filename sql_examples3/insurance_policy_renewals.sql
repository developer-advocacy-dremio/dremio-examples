/*
 * Dremio "Messy Data" Challenge: Insurance Policy Renewals
 * 
 * Scenario: 
 * Policy data with Effective/Expiration dates.
 * Gaps in coverage (Expired before Renewed).
 * Overlaps (Renewed before Expired).
 * Policy Numbers change format (POL-123 vs P-123-A).
 * 
 * Objective for AI Agent:
 * 1. Normalize Policy Numbers (extract base ID number).
 * 2. Identify Coverage Gaps: Where Start_Date > Previous End_Date.
 * 3. Identify Overlaps: Where Start_Date < Previous End_Date.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Insure_Co;
CREATE FOLDER IF NOT EXISTS Insure_Co.Policies;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Insure_Co.Policies.TERM_LIFE (
    POL_NUM VARCHAR,
    CUST_ID VARCHAR,
    EFF_DT DATE,
    EXP_DT DATE,
    PREMIUM DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Gaps, Overlaps)
-------------------------------------------------------------------------------

-- Continuous Coverage
INSERT INTO Insure_Co.Policies.TERM_LIFE VALUES
('POL-100', 'C-1', '2020-01-01', '2020-12-31', 500.0),
('POL-100', 'C-1', '2021-01-01', '2021-12-31', 520.0);

-- Gap (Lapsed)
INSERT INTO Insure_Co.Policies.TERM_LIFE VALUES
('POL-200', 'C-2', '2020-01-01', '2020-12-31', 500.0),
('POL-200', 'C-2', '2021-02-01', '2021-12-31', 600.0); -- 1 month gap

-- Overlap (Double Pay)
INSERT INTO Insure_Co.Policies.TERM_LIFE VALUES
('POL-300', 'C-3', '2020-01-01', '2020-12-31', 500.0),
('POL-300', 'C-3', '2020-12-01', '2021-12-01', 500.0); -- Overlap Dec

-- ID Format Drift
INSERT INTO Insure_Co.Policies.TERM_LIFE VALUES
('P-400-A', 'C-4', '2020-01-01', '2020-12-31', 100.0),
('POL-400', 'C-4', '2021-01-01', '2021-12-31', 110.0); -- Same base ID 400

-- Bulk Fill
INSERT INTO Insure_Co.Policies.TERM_LIFE VALUES
('POL-500', 'C-5', '2020-01-01', '2020-06-30', 200.0),
('POL-500', 'C-5', '2020-07-01', '2020-12-31', 200.0),
('POL-500', 'C-5', '2021-01-01', '2021-06-30', 200.0),
('POL-500', 'C-5', '2021-07-01', '2021-12-31', 200.0),
('POL-501', 'C-6', '2020-01-01', '2020-12-31', 1000.0),
('POL-501', 'C-6', '2022-01-01', '2022-12-31', 1200.0), -- 1 Year Gap
('POL-502', 'C-7', '2020-01-01', '2020-12-31', 500.0),
('POL-502', 'C-7', '2020-06-01', '2021-06-01', 500.0), -- 6mo Overlap
('POL-503', 'C-8', '2020-01-01', NULL, 500.0), -- Null End (Active?)
('POL-504', 'C-9', NULL, '2021-01-01', 500.0), -- Null Start
('POL-505', 'C-10', '2021-01-01', '2020-01-01', 500.0), -- End before Start
('POL-506', 'C-11', '2020-01-01', '2020-12-31', 100.0),
('POL-506', 'C-11', '2020-01-01', '2020-12-31', 100.0), -- Exact Dupe
('P-600', 'C-12', '2020-01-01', '2020-12-31', 50.0),
('POL-600', 'C-12', '2021-01-01', '2021-12-31', 55.0),
('POLICY-600', 'C-12', '2022-01-01', '2022-12-31', 60.0),
('POL-700', 'C-13', '2020-01-01', '2020-01-31', 10.0),
('POL-700', 'C-13', '2020-02-01', '2020-02-28', 10.0),
('POL-700', 'C-13', '2020-03-01', '2020-03-31', 10.0),
('POL-700', 'C-13', '2020-04-01', '2020-04-30', 10.0),
('POL-700', 'C-13', '2020-05-01', '2020-05-31', 10.0),
('POL-700', 'C-13', '2020-06-05', '2020-06-30', 10.0), -- 4 day gap (June 1-4)
('POL-800', 'C-14', '2023-01-01', '2023-12-31', 0.0), -- Free?
('POL-801', 'C-15', '2023-01-01', '2023-12-31', -50.0); -- Negative Premium

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze policy continuity in Insure_Co.Policies.TERM_LIFE.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean Policy IDs (Regex Extract digits).
 *     - Flag 'Data Errors': End < Start, or Negative Premium.
 *  3. Gold: 
 *     - Identify 'Gap' events: Using LAG(), find rows where Start > Prev_End.
 *     - Identify 'Overlap' events: Start < Prev_End.
 *  
 *  Show the SQL."
 */
