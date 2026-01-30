/*
 * Biotech: CRISPR Experiment Tracking
 * 
 * Scenario:
 * Tracking the efficacy of gene-editing experiments (CRISPR-Cas9) by analyzing sequencing results for target mutations.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS BiotechDB;
CREATE FOLDER IF NOT EXISTS BiotechDB.Research;
CREATE FOLDER IF NOT EXISTS BiotechDB.Research.Bronze;
CREATE FOLDER IF NOT EXISTS BiotechDB.Research.Silver;
CREATE FOLDER IF NOT EXISTS BiotechDB.Research.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Lab Data
-------------------------------------------------------------------------------

-- Experiments Table: Study metadata
CREATE TABLE IF NOT EXISTS BiotechDB.Research.Bronze.Experiments (
    ExpID VARCHAR,
    GeneTarget VARCHAR,
    GuideRNA_Sequence VARCHAR,
    ScientistID INT,
    ExpDate DATE
);

INSERT INTO BiotechDB.Research.Bronze.Experiments VALUES
('EXP-001', 'PCSK9', 'GATCGTAGCT', 101, '2025-01-01'),
('EXP-002', 'BRCA1', 'ATCGATGCTA', 102, '2025-01-02'),
('EXP-003', 'CFTR', 'CGTAGCTAGC', 101, '2025-01-03'),
('EXP-004', 'MYBPC3', 'GCTAGCTACG', 103, '2025-01-04'),
('EXP-005', 'PCSK9', 'TTGACGTACG', 102, '2025-01-05'), -- New guide for same target
('EXP-006', 'DMD', 'CCTAGCTAGG', 104, '2025-01-06'),
('EXP-007', 'HBB', 'AAATCGTTCG', 105, '2025-01-07'),
('EXP-008', 'HEXA', 'CCGTAGTTAG', 101, '2025-01-08'),
('EXP-009', 'BRCA2', 'GGCTATATCG', 102, '2025-01-09'),
('EXP-010', 'TP53', 'TTCGATCGAT', 103, '2025-01-10'),
('EXP-011', 'APOE', 'ATATCGGGCT', 104, '2025-01-11'),
('EXP-012', 'HTT', 'CCCGGTTTAG', 105, '2025-01-12'),
('EXP-013', 'PAH', 'GATCTAGCGA', 101, '2025-01-13'),
('EXP-014', 'F8', 'TTTCCCGGGA', 102, '2025-01-14'),
('EXP-015', 'KRAS', 'AAAGGGTTTC', 103, '2025-01-15'),
('EXP-016', 'EGFR', 'CCCTTTGGGA', 104, '2025-01-16'),
('EXP-017', 'VEGFA', 'GGGAAAT T T C', 105, '2025-01-17'),
('EXP-018', 'APP', 'TACGTACGTC', 101, '2025-01-18'),
('EXP-019', 'PSEN1', 'CGTGCGTGCG', 102, '2025-01-19'),
('EXP-020', 'LRRK2', 'TGCATGCATG', 103, '2025-01-20'),
('EXP-021', 'SOD1', 'GCGCGCGCGC', 104, '2025-01-21'),
('EXP-022', 'TARDBP', 'ATATATATAT', 105, '2025-01-22'),
('EXP-023', 'C9orf72', 'GCGCATATGC', 101, '2025-01-23'),
('EXP-024', 'FUS', 'TATAGCGCGA', 102, '2025-01-24'),
('EXP-025', 'P53', 'CGATCGATCG', 103, '2025-01-25'),
('EXP-026', 'RB1', 'GTAGCTAGCT', 104, '2025-01-26'),
('EXP-027', 'WT1', 'AGCTAGCTAG', 105, '2025-01-27'),
('EXP-028', 'NF1', 'CTAGCTAGCT', 101, '2025-01-28'),
('EXP-029', 'NF2', 'TAGCTAGCTA', 102, '2025-01-29'),
('EXP-030', 'VHL', 'GCTAGCTAGC', 103, '2025-01-30'),
('EXP-031', 'TSC1', 'ATCGATCGAT', 104, '2025-01-31'),
('EXP-032', 'TSC2', 'TCGATCGATC', 105, '2025-02-01'),
('EXP-033', 'PTEN', 'GATCGATCGA', 101, '2025-02-02'),
('EXP-034', 'CDK4', 'ATCGATCGAT', 102, '2025-02-03'),
('EXP-035', 'CDKN2A', 'CGATCGATCG', 103, '2025-02-04'),
('EXP-036', 'MEN1', 'GATCGATCGA', 104, '2025-02-05'),
('EXP-037', 'RET', 'ATCGATCGAT', 105, '2025-02-06'),
('EXP-038', 'ATM', 'CGATCGATCG', 101, '2025-02-07'),
('EXP-039', 'MLH1', 'GATCGATCGA', 102, '2025-02-08'),
('EXP-040', 'MSH2', 'ATCGATCGAT', 103, '2025-02-09'),
('EXP-041', 'MSH6', 'CGATCGATCG', 104, '2025-02-10'),
('EXP-042', 'PMS2', 'GATCGATCGA', 105, '2025-02-11'),
('EXP-043', 'EPCAM', 'ATCGATCGAT', 101, '2025-02-12'),
('EXP-044', 'APC', 'CGATCGATCG', 102, '2025-02-13'),
('EXP-045', 'MUTYH', 'GATCGATCGA', 103, '2025-02-14'),
('EXP-046', 'STK11', 'ATCGATCGAT', 104, '2025-02-15'),
('EXP-047', 'BMPR1A', 'CGATCGATCG', 105, '2025-02-16'),
('EXP-048', 'SMAD4', 'GATCGATCGA', 101, '2025-02-17'),
('EXP-049', 'ACTA2', 'ATCGATCGAT', 102, '2025-02-18'),
('EXP-050', 'MYH11', 'CGATCGATCG', 103, '2025-02-19');

-- SequencingResults Table: Outcome of editing
CREATE TABLE IF NOT EXISTS BiotechDB.Research.Bronze.SequencingResults (
    ResultID INT,
    ExpID VARCHAR,
    SampleID VARCHAR,
    Outcome VARCHAR, -- 'Successful Edit', 'Off-Target', 'No Edit'
    IndelSize INT -- Insertion/Deletion size in base pairs
);

INSERT INTO BiotechDB.Research.Bronze.SequencingResults VALUES
(1, 'EXP-001', 'S-001', 'Successful Edit', -3),
(2, 'EXP-001', 'S-002', 'Successful Edit', -4),
(3, 'EXP-001', 'S-003', 'No Edit', 0),
(4, 'EXP-001', 'S-004', 'Off-Target', 0),
(5, 'EXP-002', 'S-005', 'Successful Edit', 2),
(6, 'EXP-002', 'S-006', 'Successful Edit', 1),
(7, 'EXP-003', 'S-007', 'No Edit', 0),
(8, 'EXP-003', 'S-008', 'No Edit', 0), -- Failed guide?
(9, 'EXP-004', 'S-009', 'Successful Edit', -1),
(10, 'EXP-004', 'S-010', 'Off-Target', -15), -- Dangerous off-target
(11, 'EXP-005', 'S-011', 'Successful Edit', -2),
(12, 'EXP-005', 'S-012', 'Successful Edit', -2),
(13, 'EXP-006', 'S-013', 'Successful Edit', 5),
(14, 'EXP-007', 'S-014', 'Successful Edit', -1),
(15, 'EXP-008', 'S-015', 'No Edit', 0),
(16, 'EXP-009', 'S-016', 'Off-Target', 2),
(17, 'EXP-010', 'S-017', 'Successful Edit', -5),
(18, 'EXP-011', 'S-018', 'Successful Edit', 3),
(19, 'EXP-012', 'S-019', 'No Edit', 0),
(20, 'EXP-013', 'S-020', 'Successful Edit', -1),
(21, 'EXP-014', 'S-021', 'Successful Edit', 1),
(22, 'EXP-015', 'S-022', 'Off-Target', -10),
(23, 'EXP-016', 'S-023', 'Successful Edit', -2),
(24, 'EXP-017', 'S-024', 'Successful Edit', 4),
(25, 'EXP-018', 'S-025', 'No Edit', 0),
(26, 'EXP-019', 'S-026', 'Successful Edit', -3),
(27, 'EXP-020', 'S-027', 'Successful Edit', -4),
(28, 'EXP-021', 'S-028', 'Off-Target', 5),
(29, 'EXP-022', 'S-029', 'Successful Edit', -2),
(30, 'EXP-023', 'S-030', 'Successful Edit', 1),
(31, 'EXP-024', 'S-031', 'No Edit', 0),
(32, 'EXP-025', 'S-032', 'Successful Edit', -6),
(33, 'EXP-026', 'S-033', 'Successful Edit', 2),
(34, 'EXP-027', 'S-034', 'Successful Edit', -1),
(35, 'EXP-028', 'S-035', 'Off-Target', 0),
(36, 'EXP-029', 'S-036', 'Successful Edit', -2),
(37, 'EXP-030', 'S-037', 'Successful Edit', 3),
(38, 'EXP-031', 'S-038', 'No Edit', 0),
(39, 'EXP-032', 'S-039', 'Successful Edit', -4),
(40, 'EXP-033', 'S-040', 'Successful Edit', -5),
(41, 'EXP-034', 'S-041', 'Off-Target', 6),
(42, 'EXP-035', 'S-042', 'Successful Edit', 1),
(43, 'EXP-036', 'S-043', 'Successful Edit', -1),
(44, 'EXP-037', 'S-044', 'No Edit', 0),
(45, 'EXP-038', 'S-045', 'Successful Edit', 2),
(46, 'EXP-039', 'S-046', 'Successful Edit', -3),
(47, 'EXP-040', 'S-047', 'Successful Edit', -2),
(48, 'EXP-041', 'S-048', 'Off-Target', -8),
(49, 'EXP-042', 'S-049', 'Successful Edit', 1),
(50, 'EXP-043', 'S-050', 'Successful Edit', -1),
(51, 'EXP-044', 'S-051', 'No Edit', 0),
(52, 'EXP-045', 'S-052', 'Successful Edit', 4),
(53, 'EXP-046', 'S-053', 'Successful Edit', -5),
(54, 'EXP-047', 'S-054', 'Successful Edit', 2),
(55, 'EXP-048', 'S-055', 'Off-Target', 10),
(56, 'EXP-049', 'S-056', 'Successful Edit', -2),
(57, 'EXP-050', 'S-057', 'Successful Edit', 0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Efficacy Calculation
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BiotechDB.Research.Silver.ExperimentalOutcomes AS
SELECT 
    e.ExpID,
    e.GeneTarget,
    e.ScientistID,
    r.SampleID,
    r.Outcome,
    r.IndelSize
FROM BiotechDB.Research.Bronze.Experiments e
JOIN BiotechDB.Research.Bronze.SequencingResults r ON e.ExpID = r.ExpID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Efficacy Benchmarks
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BiotechDB.Research.Gold.GuideRNAEfficacy AS
SELECT 
    GeneTarget,
    ExpID,
    COUNT(*) AS TotalSamples,
    SUM(CASE WHEN Outcome = 'Successful Edit' THEN 1 ELSE 0 END) AS SuccessCount,
    SUM(CASE WHEN Outcome = 'Off-Target' THEN 1 ELSE 0 END) AS OffTargetCount,
    (CAST(SUM(CASE WHEN Outcome = 'Successful Edit' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) * 100 AS SuccessRatePct
FROM BiotechDB.Research.Silver.ExperimentalOutcomes
GROUP BY GeneTarget, ExpID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify Gene Targets with a success rate lower than 50% using BiotechDB.Research.Gold.GuideRNAEfficacy."

PROMPT 2:
"List all 'Off-Target' outcomes including the IndelSize from the Silver layer."

PROMPT 3:
"Show the experiment results for Gene Target 'PCSK9'."
*/
