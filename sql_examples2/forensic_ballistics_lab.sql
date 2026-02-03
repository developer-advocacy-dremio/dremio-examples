/*
 * Dremio Forensic Ballistics Lab Example
 * 
 * Domain: Law Enforcement & Forensics
 * Scenario: 
 * A Crime Lab scans striations (micro-grooves) on bullets recovered from crime scenes.
 * These are matched against a "Firearm Database" of registered guns and previous test-fires.
 * High "Match Scores" indicate a likely link between different crimes or a specific suspect's weapon.
 * 
 * Complexity: Medium (Pattern matching join simulation, threshold filtering)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Crime_Lab;
CREATE FOLDER IF NOT EXISTS Crime_Lab.Sources;
CREATE FOLDER IF NOT EXISTS Crime_Lab.Bronze;
CREATE FOLDER IF NOT EXISTS Crime_Lab.Silver;
CREATE FOLDER IF NOT EXISTS Crime_Lab.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Crime_Lab.Sources.Case_Evidence (
    EvidenceID VARCHAR,
    CaseID VARCHAR,
    Recovery_Date DATE,
    Item_Type VARCHAR, -- 'Bullet', 'Casing'
    Caliber VARCHAR, -- '9mm', '45ACP'
    Striation_Signature_Hash VARCHAR -- Simulating matching data
);

CREATE TABLE IF NOT EXISTS Crime_Lab.Sources.Firearm_Registry (
    GunID VARCHAR,
    Serial_Number VARCHAR,
    Make VARCHAR,
    Model VARCHAR,
    Caliber VARCHAR,
    Owner_Name VARCHAR, -- 'Police Dept', 'Private', 'Unregistered'
    Test_Fire_Signature_Hash VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Firearms (Known sources)
INSERT INTO Crime_Lab.Sources.Firearm_Registry VALUES
('GUN-001', 'SN-12345', 'Glock', '19', '9mm', 'John Doe', 'HASH-A1B2'),
('GUN-002', 'SN-67890', 'Sig Sauer', 'P320', '9mm', 'Jane Smith', 'HASH-C3D4'),
('GUN-003', 'SN-54321', 'Colt', '1911', '45ACP', 'Bob Jones', 'HASH-E5F6'),
('GUN-004', 'SN-09876', 'Smith & Wesson', 'M&P', '9mm', 'Alice White', 'HASH-G7H8'),
('GUN-005', 'SN-11223', 'Beretta', '92FS', '9mm', 'Police Dept', 'HASH-T9U0'), -- Police Issue
('GUN-006', 'SN-33445', 'Ruger', 'LCP', '380', 'Private', 'HASH-XXX1'),
('GUN-007', 'SN-55667', 'Glock', '17', '9mm', 'Private', 'HASH-YYY2'),
('GUN-008', 'SN-77889', 'HK', 'VP9', '9mm', 'Private', 'HASH-ZZZ3'),
('GUN-009', 'SN-99000', 'Walther', 'PPQ', '9mm', 'Private', 'HASH-AAA4'),
('GUN-010', 'SN-11111', 'Taurus', 'G2C', '9mm', 'Private', 'HASH-BBB5'),
('GUN-011', 'SN-22222', 'Colt', 'Python', '357', 'Private', 'HASH-CCC6'),
('GUN-012', 'SN-33333', 'Mossberg', '500', '12Ga', 'Private', 'HASH-DDD7'),
('GUN-013', 'SN-44444', 'Remington', '870', '12Ga', 'Private', 'HASH-EEE8'),
('GUN-014', 'SN-55555', 'Winchester', '94', '30-30', 'Private', 'HASH-FFF9'),
('GUN-015', 'SN-66666', 'Browning', 'Hi-Power', '9mm', 'Private', 'HASH-GGG0');

-- Seed Evidence (Matches and Non-Matches)
-- HASH-A1B2 (Gun-001) used in crimes?
INSERT INTO Crime_Lab.Sources.Case_Evidence VALUES
('EVID-101', 'CASE-2023-001', '2023-01-01', 'Bullet', '9mm', 'HASH-A1B2'), -- Match GUN-001
('EVID-102', 'CASE-2023-002', '2023-01-15', 'Casing', '9mm', 'HASH-A1B2'), -- Match GUN-001 (Same gun, diff crime)
('EVID-103', 'CASE-2023-003', '2023-02-01', 'Bullet', '45ACP', 'HASH-E5F6'), -- Match GUN-003
('EVID-104', 'CASE-2023-004', '2023-02-10', 'Bullet', '9mm', 'HASH-XYZ9'), -- No Match (Ghost Gun?)
('EVID-105', 'CASE-2023-005', '2023-03-01', 'Casing', '9mm', 'HASH-XYZ9'), -- Link to Case 004!
('EVID-106', 'CASE-2023-006', '2023-03-15', 'Bullet', '380', 'HASH-XXX1'), -- Match GUN-006
('EVID-107', 'CASE-2023-007', '2023-04-01', 'Casing', '9mm', 'HASH-C3D4'), -- Match GUN-002
('EVID-108', 'CASE-2023-008', '2023-04-05', 'Bullet', '9mm', 'HASH-RES1'), -- No match
('EVID-109', 'CASE-2023-009', '2023-04-10', 'Bullet', '9mm', 'HASH-RES2'),
('EVID-110', 'CASE-2023-010', '2023-04-15', 'Casing', '45ACP', 'HASH-RES3'),
('EVID-111', 'CASE-2023-011', '2023-05-01', 'Bullet', '9mm', 'HASH-A1B2'), -- Gun-001 again! Serial offender
('EVID-112', 'CASE-2023-012', '2023-05-05', 'Bullet', '9mm', 'HASH-G7H8'), -- Match GUN-004
('EVID-113', 'CASE-2023-013', '2023-05-10', 'Casing', '9mm', 'HASH-G7H8'),
('EVID-114', 'CASE-2023-014', '2023-05-15', 'Bullet', '9mm', 'HASH-T9U0'), -- Police gun
('EVID-115', 'CASE-2023-015', '2023-06-01', 'Bullet', '9mm', 'HASH-YYY2'), -- Match GUN-007
('EVID-116', 'CASE-2023-016', '2023-06-05', 'Casing', '9mm', 'HASH-YYY2'),
('EVID-117', 'CASE-2023-017', '2023-06-10', 'Bullet', '9mm', 'HASH-ZZZ3'),
('EVID-118', 'CASE-2023-018', '2023-06-15', 'Bullet', '9mm', 'HASH-AAA4'),
('EVID-119', 'CASE-2023-019', '2023-06-20', 'Casing', '9mm', 'HASH-BBB5'),
('EVID-120', 'CASE-2023-020', '2023-06-25', 'Bullet', '357', 'HASH-CCC6'),
('EVID-121', 'CASE-2023-021', '2023-07-01', 'Casing', '9mm', 'HASH-RES4'), -- Unmatched
('EVID-122', 'CASE-2023-022', '2023-07-05', 'Bullet', '9mm', 'HASH-RES4'), -- Linked to 021
('EVID-123', 'CASE-2023-023', '2023-07-10', 'Bullet', '9mm', 'HASH-RES4'), -- Serial
('EVID-124', 'CASE-2023-024', '2023-07-15', 'Bullet', '9mm', 'HASH-A1B2'), -- Gun-001 again...
('EVID-125', 'CASE-2023-025', '2023-08-01', 'Casing', '45ACP', 'HASH-E5F6'), -- Gun-003 again
('EVID-126', 'CASE-2023-026', '2023-08-05', 'Bullet', '9mm', 'HASH-UNK1'),
('EVID-127', 'CASE-2023-027', '2023-08-10', 'Bullet', '9mm', 'HASH-UNK2'),
('EVID-128', 'CASE-2023-028', '2023-08-15', 'Casing', '9mm', 'HASH-UNK3'),
('EVID-129', 'CASE-2023-029', '2023-08-20', 'Bullet', '380', 'HASH-XXX1'),
('EVID-130', 'CASE-2023-030', '2023-08-25', 'Bullet', '9mm', 'HASH-YYY2'),
('EVID-131', 'CASE-2023-031', '2023-09-01', 'Casing', '9mm', 'HASH-G7H8'),
('EVID-132', 'CASE-2023-032', '2023-09-05', 'Bullet', '9mm', 'HASH-G7H8'),
('EVID-133', 'CASE-2023-033', '2023-09-10', 'Bullet', '9mm', 'HASH-A1B2'), -- Prolific
('EVID-134', 'CASE-2023-034', '2023-09-15', 'Casing', '9mm', 'HASH-A1B2'),
('EVID-135', 'CASE-2023-035', '2023-09-20', 'Bullet', '12Ga', 'HASH-DDD7'),
('EVID-136', 'CASE-2023-036', '2023-10-01', 'Bullet', '12Ga', 'HASH-EEE8'),
('EVID-137', 'CASE-2023-037', '2023-10-05', 'Bullet', '30-30', 'HASH-FFF9'),
('EVID-138', 'CASE-2023-038', '2023-10-10', 'Bullet', '9mm', 'HASH-GGG0'),
('EVID-139', 'CASE-2023-039', '2023-10-15', 'Casing', '9mm', 'HASH-GGG0'),
('EVID-140', 'CASE-2023-040', '2023-10-20', 'Bullet', '9mm', 'HASH-RES5'),
('EVID-141', 'CASE-2023-041', '2023-11-01', 'Bullet', '9mm', 'HASH-A1B2'),
('EVID-142', 'CASE-2023-042', '2023-11-05', 'Casing', '9mm', 'HASH-A1B2'),
('EVID-143', 'CASE-2023-043', '2023-11-10', 'Bullet', '9mm', 'HASH-A1B2'),
('EVID-144', 'CASE-2023-044', '2023-11-15', 'Bullet', '9mm', 'HASH-UNK9'),
('EVID-145', 'CASE-2023-045', '2023-11-20', 'Casing', '9mm', 'HASH-UNK9'), -- Linked Unknowns
('EVID-146', 'CASE-2023-046', '2023-12-01', 'Bullet', '9mm', 'HASH-A1B2'),
('EVID-147', 'CASE-2023-047', '2023-12-05', 'Bullet', '9mm', 'HASH-ZZZ3'),
('EVID-148', 'CASE-2023-048', '2023-12-10', 'Bullet', '9mm', 'HASH-ZZZ3'),
('EVID-149', 'CASE-2023-049', '2023-12-15', 'Casing', '9mm', 'HASH-AAA4'),
('EVID-150', 'CASE-2023-050', '2023-12-20', 'Bullet', '9mm', 'HASH-AAA4');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Crime_Lab.Bronze.Bronze_Evidence AS SELECT * FROM Crime_Lab.Sources.Case_Evidence;
CREATE OR REPLACE VIEW Crime_Lab.Bronze.Bronze_Registry AS SELECT * FROM Crime_Lab.Sources.Firearm_Registry;

-- 4b. SILVER LAYER (NIBIN Hits)
CREATE OR REPLACE VIEW Crime_Lab.Silver.Silver_Ballistics_Matches AS
SELECT
    e.EvidenceID,
    e.CaseID,
    e.Recovery_Date,
    e.Item_Type,
    e.Striation_Signature_Hash,
    r.GunID,
    r.Owner_Name,
    r.Make,
    r.Model,
    CASE 
        WHEN r.GunID IS NOT NULL THEN 'Positive ID'
        ELSE 'Unmatched'
    END as Match_Status
FROM Crime_Lab.Bronze.Bronze_Evidence e
LEFT JOIN Crime_Lab.Bronze.Bronze_Registry r 
  ON e.Striation_Signature_Hash = r.Test_Fire_Signature_Hash;

-- 4c. GOLD LAYER (Linkage Analysis)
CREATE OR REPLACE VIEW Crime_Lab.Gold.Gold_Crime_Gun_Linkage AS
SELECT
    COALESCE(GunID, 'Unknown Gun (' || Striation_Signature_Hash || ')') as Weapon_Identifier,
    COUNT(DISTINCT CaseID) as Linked_Cases,
    LISTAGG(DISTINCT CaseID, ', ') as Case_List,
    MIN(Recovery_Date) as First_Seen,
    MAX(Recovery_Date) as Last_Seen,
    Owner_Name
FROM Crime_Lab.Silver.Silver_Ballistics_Matches
GROUP BY Weapon_Identifier, Striation_Signature_Hash, GunID, Owner_Name
HAVING COUNT(DISTINCT CaseID) > 1 -- Identify serial weapons
ORDER BY Linked_Cases DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Gold_Crime_Gun_Linkage' to map out the heatmap of 'Unmatched' weapons with > 3 linked cases. 
 * This suggests a serial offender operating off the radar."
 */
