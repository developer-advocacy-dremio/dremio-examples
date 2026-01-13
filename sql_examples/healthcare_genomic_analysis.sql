/*
 * Genomic Data Analysis Demo
 * 
 * Scenario:
 * Analyzing DNA variant logs and gene expression levels (Simulated).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Genomics;
CREATE FOLDER IF NOT EXISTS RetailDB.Genomics.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Genomics.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Genomics.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Genomics.Bronze.VariantLogs (
    SampleID VARCHAR,
    GeneSymbol VARCHAR,
    VariantType VARCHAR, -- SNP, INDEL
    Impact VARCHAR, -- HIGH, MODERATE, LOW
    ReadDepth INT
);

INSERT INTO RetailDB.Genomics.Bronze.VariantLogs VALUES
('S001', 'BRCA1', 'SNP', 'HIGH', 150),
('S001', 'TP53', 'SNP', 'MODERATE', 120),
('S002', 'BRCA2', 'INDEL', 'HIGH', 200),
('S002', 'EGFR', 'SNP', 'LOW', 80),
('S003', 'TP53', 'SNP', 'HIGH', 140),
('S003', 'KRAS', 'SNP', 'MODERATE', 90),
('S004', 'BRAF', 'SNP', 'HIGH', 160),
('S005', 'BRCA1', 'INDEL', 'MODERATE', 110),
('S006', 'TP53', 'SNP', 'LOW', 70),
('S007', 'EGFR', 'SNP', 'HIGH', 180),
('S008', 'KRAS', 'INDEL', 'HIGH', 190),
('S009', 'BRAF', 'SNP', 'LOW', 60);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Genomics.Silver.HighImpactVariants AS
SELECT 
    SampleID,
    GeneSymbol,
    VariantType,
    ReadDepth
FROM RetailDB.Genomics.Bronze.VariantLogs
WHERE Impact = 'HIGH' AND ReadDepth > 100;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Genomics.Gold.GeneMutationFreq AS
SELECT 
    GeneSymbol,
    COUNT(*) AS MutationCount,
    AVG(ReadDepth) AS AvgDepth
FROM RetailDB.Genomics.Silver.HighImpactVariants
GROUP BY GeneSymbol;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which gene has the most high-impact mutations in RetailDB.Genomics.Gold.GeneMutationFreq?"
*/
