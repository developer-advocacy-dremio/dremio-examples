/*
 * Dremio "Messy Data" Challenge: Genomic Sequence Alignment
 * 
 * Scenario: 
 * DNA sequencing data (Strings of A, C, G, T).
 * Contains 'N' (Unknown base) noise.
 * Some sequences are fragments (too short) or have invalid chars ('X', 'Z').
 * 
 * Objective for AI Agent:
 * 1. Validate DNA: Check length > 50 and sequence only contains {A,C,G,T,N}.
 * 2. Calculate GC Content: % of 'G' and 'C' bases (Indicator of stability).
 * 3. Filter 'Low Quality': Sequences with > 10% 'N'.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Bio_Lab;
CREATE FOLDER IF NOT EXISTS Bio_Lab.Sequencer;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Bio_Lab.Sequencer.READS (
    READ_ID VARCHAR,
    INSTRUMENT_ID VARCHAR,
    DNA_SEQ VARCHAR, -- The raw sequence
    QUALITY_SCORE DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Noise, Fragments)
-------------------------------------------------------------------------------

-- Good Reads
INSERT INTO Bio_Lab.Sequencer.READS VALUES
('R-1', 'Inst-A', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', 0.99), -- 50 chars
('R-2', 'Inst-A', 'GGGGCCCCGGGGCCCCGGGGCCCCGGGGCCCCGGGGCCCCGGGGCCCCGG', 0.98);

-- Noisy (N)
INSERT INTO Bio_Lab.Sequencer.READS VALUES
('R-3', 'Inst-B', 'ATCGNNNNATCGNNNNATCGNNNNATCGNNNNATCGNNNNATCGNNNNAT', 0.50); -- High noise

-- Invalid Chars
INSERT INTO Bio_Lab.Sequencer.READS VALUES
('R-4', 'Inst-B', 'ATCGXYZATCGXYZATCGATCGATCGATCGATCGATCGATCGATCGATCG', 0.00);

-- Too Short
INSERT INTO Bio_Lab.Sequencer.READS VALUES
('R-5', 'Inst-A', 'ATCG', 0.99);

-- Bulk Fill
INSERT INTO Bio_Lab.Sequencer.READS VALUES
('R-6', 'Inst-A', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 0.95),
('R-6', 'Inst-A', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 0.95), -- Dupe
('R-7', 'Inst-A', 'TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT', 0.95),
('R-8', 'Inst-A', 'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC', 0.95),
('R-9', 'Inst-A', 'GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG', 0.95),
('R-10', 'Inst-B', 'ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTAC', 0.90),
('R-11', 'Inst-B', 'TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATG', 0.90),
('R-12', 'Inst-B', 'NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN', 0.01), -- All N
('R-13', 'Inst-C', 'ATATATATATATATATATATATATATATATATATATATATATATATATAT', 0.85),
('R-14', 'Inst-C', 'CGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCG', 0.85),
('R-15', 'Inst-C', 'AAAAACCCCCGGGGGTTTTTAAAAACCCCCGGGGGTTTTTAAAAACCCCC', 0.88),
('R-16', 'Inst-A', 'GATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGA', 0.92),
('R-17', 'Inst-A', 'CTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCT', 0.92),
('R-18', 'Inst-B', 'AANNAANNAANNAANNAANNAANNAANNAANNAANNAANNAANNAANNAA', 0.60),
('R-19', 'Inst-B', 'CCNNCCNNCCNNCCNNCCNNCCNNCCNNCCNNCCNNCCNNCCNNCCNNCC', 0.60),
('R-20', 'Inst-B', 'GGNNGGNNGGNNGGNNGGNNGGNNGGNNGGNNGGNNGGNNGGNNGGNNGG', 0.60),
('R-21', 'Inst-C', 'TTNNTTNNTTNNTTNNTTNNTTNNTTNNTTNNTTNNTTNNTTNNTTNNTT', 0.60),
('R-22', 'Inst-C', 'UnknownSequence', 0.0), -- Text string
('R-23', 'Inst-C', NULL, 0.0),
('R-24', 'Inst-A', 'AaCcGgTtAaCcGgTtAaCcGgTtAaCcGgTtAaCcGgTtAaCcGgTtAa', 0.95), -- Mixed case
('R-25', 'Inst-A', '  ATCG  ', 0.90), -- Whitespace
('R-26', 'Inst-A', 'ATCG-ATCG-ATCG-ATCG-ATCG-ATCG-ATCG-ATCG-ATCG-ATCG-', 0.80), -- Dashes
('R-27', 'Inst-B', '12345', 0.0), -- Numbers
('R-28', 'Inst-B', 'ATCG!', 0.98), -- Special char
('R-29', 'Inst-C', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', -1.0), -- Neg Quality
('R-30', 'Inst-C', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', 100.0), -- Bad Quality scale
('R-31', 'Inst-C', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', 2.0),
('R-32', 'Inst-A', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', 0.0),
('R-33', 'Inst-A', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', 0.5),
('R-34', 'Inst-A', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', 0.5), -- Dupe
('R-35', 'Inst-A', 'AT', 0.99), -- Short
('R-36', 'Inst-A', 'ATC', 0.99), -- Short
('R-37', 'Inst-A', 'ATCG', 0.99), -- Short
('R-38', 'Inst-A', 'ATCGA', 0.99), -- Short
('R-39', 'Inst-A', 'ATCGAT', 0.99), -- Short
('R-40', 'Inst-B', 'ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGAT', 0.9);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Process DNA reads in Bio_Lab.Sequencer.READS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean Sequence: Upper case, trim whitespace.
 *     - Validate: Length > 10. Only characters A,C,G,T,N allowed.
 *     - Calculate 'GC_Content': (Count(G) + Count(C)) / Length.
 *     - Calculate 'N_Ratio': Count(N) / Length.
 *  3. Gold: 
 *     - Filter 'High Quality': N_Ratio < 0.05 AND Quality_Score > 0.9.
 *  
 *  Show the SQL."
 */
