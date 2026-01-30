/*
 * LegalTech: E-Discovery Relevance Scoring
 * 
 * Scenario:
 * Calculating relevance scores for litigation documents based on keyword density, metadata, and custodian importance.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS LegalDiscoveryDB;
CREATE FOLDER IF NOT EXISTS LegalDiscoveryDB.Litigation;
CREATE FOLDER IF NOT EXISTS LegalDiscoveryDB.Litigation.Bronze;
CREATE FOLDER IF NOT EXISTS LegalDiscoveryDB.Litigation.Silver;
CREATE FOLDER IF NOT EXISTS LegalDiscoveryDB.Litigation.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Document Metadata
-------------------------------------------------------------------------------

-- DocMetadata Table
CREATE TABLE IF NOT EXISTS LegalDiscoveryDB.Litigation.Bronze.DocMetadata (
    DocID VARCHAR,
    Custodian VARCHAR, -- CEO, CFO, Engineer
    FileType VARCHAR, -- Email, PDF, Spreadsheet
    KeywordHits INT, -- Number of responsive terms found
    IsPrivileged BOOLEAN,
    DocTimestamp TIMESTAMP
);

INSERT INTO LegalDiscoveryDB.Litigation.Bronze.DocMetadata VALUES
('D-001', 'CEO', 'Email', 15, false, '2024-05-01 09:00:00'),
('D-002', 'CFO', 'Spreadsheet', 5, false, '2024-05-01 10:00:00'),
('D-003', 'Engineer', 'PDF', 2, false, '2024-05-01 11:00:00'),
('D-004', 'CEO', 'Email', 40, true, '2024-05-01 12:00:00'), -- Privileged
('D-005', 'Legal', 'PDF', 10, true, '2024-05-01 13:00:00'),
('D-006', 'Sales', 'Email', 1, false, '2024-05-01 14:00:00'),
('D-007', 'CEO', 'Email', 25, false, '2024-05-02 09:00:00'),
('D-008', 'CFO', 'Email', 30, false, '2024-05-02 10:00:00'),
('D-009', 'Engineer', 'Spreadsheet', 0, false, '2024-05-02 11:00:00'),
('D-010', 'Sales', 'PDF', 3, false, '2024-05-02 12:00:00'),
('D-011', 'CEO', 'Email', 12, false, '2024-05-03 09:00:00'),
('D-012', 'Engineer', 'Email', 8, false, '2024-05-03 10:00:00'),
('D-013', 'CFO', 'Spreadsheet', 18, false, '2024-05-03 11:00:00'),
('D-014', 'Legal', 'Email', 5, true, '2024-05-03 12:00:00'),
('D-015', 'CEO', 'PDF', 20, false, '2024-05-04 09:00:00'),
('D-016', 'Engineer', 'Email', 1, false, '2024-05-04 10:00:00'),
('D-017', 'Sales', 'Spreadsheet', 2, false, '2024-05-04 11:00:00'),
('D-018', 'CEO', 'Email', 35, true, '2024-05-04 12:00:00'),
('D-019', 'CFO', 'PDF', 14, false, '2024-05-05 09:00:00'),
('D-020', 'Engineer', 'Email', 6, false, '2024-05-05 10:00:00'),
('D-021', 'Sales', 'Email', 0, false, '2024-05-05 11:00:00'),
('D-022', 'CEO', 'Spreadsheet', 22, false, '2024-05-06 09:00:00'),
('D-023', 'CFO', 'Email', 28, false, '2024-05-06 10:00:00'),
('D-024', 'Legal', 'PDF', 8, true, '2024-05-06 11:00:00'),
('D-025', 'Engineer', 'Email', 9, false, '2024-05-06 12:00:00'),
('D-026', 'Sales', 'Spreadsheet', 1, false, '2024-05-07 09:00:00'),
('D-027', 'CEO', 'Email', 18, false, '2024-05-07 10:00:00'),
('D-028', 'CFO', 'PDF', 11, false, '2024-05-07 11:00:00'),
('D-029', 'Engineer', 'Email', 4, false, '2024-05-07 12:00:00'),
('D-030', 'CEO', 'Spreadsheet', 33, false, '2024-05-08 09:00:00'),
('D-031', 'Sales', 'Email', 2, false, '2024-05-08 10:00:00'),
('D-032', 'Legal', 'Email', 12, true, '2024-05-08 11:00:00'),
('D-033', 'CFO', 'Spreadsheet', 19, false, '2024-05-08 12:00:00'),
('D-034', 'CEO', 'PDF', 26, false, '2024-05-09 09:00:00'),
('D-035', 'Engineer', 'Email', 7, false, '2024-05-09 10:00:00'),
('D-036', 'Sales', 'PDF', 1, false, '2024-05-09 11:00:00'),
('D-037', 'CEO', 'Email', 29, false, '2024-05-10 09:00:00'),
('D-038', 'CFO', 'Email', 21, false, '2024-05-10 10:00:00'),
('D-039', 'Legal', 'Spreadsheet', 6, true, '2024-05-10 11:00:00'),
('D-040', 'Engineer', 'PDF', 3, false, '2024-05-10 12:00:00'),
('D-041', 'CEO', 'Email', 14, false, '2024-05-11 09:00:00'),
('D-042', 'Sales', 'Email', 0, false, '2024-05-11 10:00:00'),
('D-043', 'CFO', 'PDF', 13, false, '2024-05-11 11:00:00'),
('D-044', 'Engineer', 'Spreadsheet', 2, false, '2024-05-11 12:00:00'),
('D-045', 'CEO', 'Email', 32, true, '2024-05-12 09:00:00'),
('D-046', 'Legal', 'Email', 9, true, '2024-05-12 10:00:00'),
('D-047', 'CFO', 'Spreadsheet', 17, false, '2024-05-12 11:00:00'),
('D-048', 'Sales', 'PDF', 4, false, '2024-05-12 12:00:00'),
('D-049', 'Engineer', 'Email', 5, false, '2024-05-13 09:00:00'),
('D-050', 'CEO', 'PDF', 24, false, '2024-05-13 10:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Scoring Logic
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW LegalDiscoveryDB.Litigation.Silver.RelevanceScoring AS
SELECT 
    DocID,
    Custodian,
    FileType,
    KeywordHits,
    IsPrivileged,
    -- Weighted Scoring: (KeywordHits * 2) + (Custodian Weight)
    (KeywordHits * 2) + 
    CASE 
        WHEN Custodian IN ('CEO', 'CFO') THEN 50
        WHEN Custodian = 'Legal' THEN 30
        ELSE 10
    END AS RelevanceScore
FROM LegalDiscoveryDB.Litigation.Bronze.DocMetadata
WHERE IsPrivileged = false; -- Filter out privileged docs from general review pile

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Production Set
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW LegalDiscoveryDB.Litigation.Gold.ReviewPriority AS
SELECT 
    DocID,
    Custodian,
    RelevanceScore,
    CASE 
        WHEN RelevanceScore > 80 THEN 'Tier 1: High Priority'
        WHEN RelevanceScore > 40 THEN 'Tier 2: Medium'
        ELSE 'Tier 3: Low / Noise'
    END AS ReviewQueue
FROM LegalDiscoveryDB.Litigation.Silver.RelevanceScoring;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Count how many documents are in 'Tier 1: High Priority' for the 'CEO' custodian."

PROMPT 2:
"Calculate the average KeywordHits for 'Tier 2: Medium' documents in the Silver layer (join required or logic derived)."

PROMPT 3:
"List all non-privileged documents with a RelevanceScore greater than 70."
*/
