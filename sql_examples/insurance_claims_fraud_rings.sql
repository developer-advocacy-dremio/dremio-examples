/*
 * Insurance Claims Fraud Ring Detection Demo
 * 
 * Scenario:
 * Organized crime rings file multiple fake claims using different identities but shared infrastructure 
 * (Same IP address, Device ID, or Address).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify clusters of claims linked by shared metadata.
 * 
 * Note: Assumes a catalog named 'InsuranceFraudDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS InsuranceFraudDB;
CREATE FOLDER IF NOT EXISTS InsuranceFraudDB.Bronze;
CREATE FOLDER IF NOT EXISTS InsuranceFraudDB.Silver;
CREATE FOLDER IF NOT EXISTS InsuranceFraudDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Claim Metadata
-------------------------------------------------------------------------------
-- Description: Claims data enriched with digital footprint info.

CREATE TABLE IF NOT EXISTS InsuranceFraudDB.Bronze.Claims (
    ClaimID VARCHAR,
    PolicyID VARCHAR,
    ClaimDate DATE,
    Amount DOUBLE,
    Description VARCHAR
);

CREATE TABLE IF NOT EXISTS InsuranceFraudDB.Bronze.DeviceFingerprints (
    ClaimID VARCHAR,
    IPAddress VARCHAR,
    DeviceID VARCHAR, -- Unique hardware ID
    MACAddress VARCHAR
);

-- 1.1 Populate Claims (50+ Records)
INSERT INTO InsuranceFraudDB.Bronze.Claims (ClaimID, PolicyID, ClaimDate, Amount, Description) VALUES
('CLM-001', 'POL-100', '2025-01-01', 5000.00, 'Car Accident'),
('CLM-002', 'POL-101', '2025-01-02', 15000.00, 'Home Burglary'),
('CLM-003', 'POL-102', '2025-01-03', 2500.00, 'Windshield Damage'),
('CLM-004', 'POL-103', '2025-01-03', 12000.00, 'Medical Expense'),
('CLM-005', 'POL-104', '2025-01-04', 3000.00, 'Minor Collision'),
('CLM-006', 'POL-105', '2025-01-05', 4500.00, 'Water Damage'),
('CLM-007', 'POL-106', '2025-01-05', 8000.00, 'Theft'),
('CLM-008', 'POL-107', '2025-01-06', 200.00, 'Towing'),
('CLM-009', 'POL-108', '2025-01-07', 5500.00, 'Car Accident'),
('CLM-010', 'POL-109', '2025-01-07', 1500.00, 'Slip and Fall'),
('CLM-011', 'POL-110', '2025-01-08', 9000.00, 'Fire Damage'),
('CLM-012', 'POL-111', '2025-01-08', 350.00, 'Glass Repair'),
('CLM-013', 'POL-112', '2025-01-09', 22000.00, 'Total Loss'),
('CLM-014', 'POL-113', '2025-01-09', 1800.00, 'Hail Damage'),
('CLM-015', 'POL-114', '2025-01-10', 4000.00, 'Rear End'),
('CLM-016', 'POL-115', '2025-01-10', 600.00, 'Rental Car'),
('CLM-017', 'POL-116', '2025-01-11', 11000.00, 'Roof Damage'),
('CLM-018', 'POL-117', '2025-01-11', 7500.00, 'Vandalism'),
('CLM-019', 'POL-118', '2025-01-12', 3000.00, 'Fender Bender'),
('CLM-020', 'POL-119', '2025-01-12', 25000.00, 'Bodily Injury'),
('CLM-021', 'POL-120', '2025-01-13', 500.00, 'Locksmith'),
('CLM-022', 'POL-121', '2025-01-13', 4200.00, 'Side Swipe'),
('CLM-023', 'POL-122', '2025-01-14', 16000.00, 'Pipe Burst'),
('CLM-024', 'POL-123', '2025-01-14', 2800.00, 'Animal Collision'),
('CLM-025', 'POL-124', '2025-01-15', 6500.00, 'Identify Theft'),
('CLM-026', 'POL-125', '2025-01-15', 1200.00, 'Bike Theft'),
('CLM-027', 'POL-126', '2025-01-16', 3300.00, 'Car Accident'),
('CLM-028', 'POL-127', '2025-01-16', 950.00, 'Electronics Damage'),
('CLM-029', 'POL-128', '2025-01-17', 21000.00, 'Structural Damage'),
('CLM-030', 'POL-129', '2025-01-17', 100.00, 'Roadside Assist'),
('CLM-031', 'POL-130', '2025-01-18', 4800.00, 'Parking Lot Hit'),
('CLM-032', 'POL-131', '2025-01-18', 13000.00, 'Storm Damage'),
('CLM-033', 'POL-132', '2025-01-19', 2400.00, 'Engine Fire'),
('CLM-034', 'POL-133', '2025-01-19', 3100.00, 'Fence Damage'),
('CLM-035', 'POL-134', '2025-01-20', 5000.00, 'Car Accident'), -- Suspect Ring Start
('CLM-036', 'POL-135', '2025-01-20', 4900.00, 'Car Accident'), -- Suspect
('CLM-037', 'POL-136', '2025-01-20', 5100.00, 'Car Accident'), -- Suspect
('CLM-038', 'POL-137', '2025-01-21', 1500.00, 'Laptop Theft'),
('CLM-039', 'POL-138', '2025-01-21', 800.00, 'Tablets Theft'),
('CLM-040', 'POL-139', '2025-01-22', 17000.00, 'Flood'),
('CLM-041', 'POL-140', '2025-01-22', 2900.00, 'Tree Fall'),
('CLM-042', 'POL-141', '2025-01-23', 5300.00, 'Car Accident'), -- Suspect
('CLM-043', 'POL-142', '2025-01-23', 6000.00, 'Jewelry Theft'),
('CLM-044', 'POL-143', '2025-01-24', 450.00, 'Scratch Repair'),
('CLM-045', 'POL-144', '2025-01-24', 9200.00, 'Kitchen Fire'),
('CLM-046', 'POL-145', '2025-01-25', 1100.00, 'Camera Theft'),
('CLM-047', 'POL-146', '2025-01-25', 3600.00, 'Garage Door'),
('CLM-048', 'POL-147', '2025-01-26', 14000.00, 'Foundation Crack'),
('CLM-049', 'POL-148', '2025-01-26', 250.00, 'Key Loss'),
('CLM-050', 'POL-149', '2025-01-27', 7000.00, 'Artwork Theft');

-- 1.2 Populate DeviceFingerprints (Samples with Shared IPs)
INSERT INTO InsuranceFraudDB.Bronze.DeviceFingerprints (ClaimID, IPAddress, DeviceID, MACAddress) VALUES
('CLM-001', '192.168.1.101', 'DEV-A', 'AA:BB:CC:01'),
('CLM-002', '192.168.1.102', 'DEV-B', 'AA:BB:CC:02'),
('CLM-035', '203.0.113.55', 'BAD-DEV-1', 'DE:AD:BE:EF:01'), -- Ring Node 1
('CLM-036', '203.0.113.55', 'BAD-DEV-2', 'DE:AD:BE:EF:02'), -- Ring Node 2 (Shared IP)
('CLM-037', '198.51.100.22', 'BAD-DEV-1', 'DE:AD:BE:EF:01'), -- Ring Node 3 (Shared DeviceID with 1)
('CLM-042', '203.0.113.55', 'BAD-DEV-3', 'DE:AD:BE:EF:03'), -- Ring Node 4 (Shared IP)
('CLM-003', '10.0.0.5', 'DEV-C', 'AA:BB:CC:03'),
('CLM-004', '10.0.0.6', 'DEV-D', 'AA:BB:CC:04');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Network Graph
-------------------------------------------------------------------------------
-- Description: Finding links between claims based on shared metadata.
-- Feature: Self-joins to find collision on IP or DeviceID.

CREATE OR REPLACE VIEW InsuranceFraudDB.Silver.LinkedClaims AS
SELECT
    a.ClaimID AS ClaimA,
    b.ClaimID AS ClaimB,
    a.IPAddress AS SharedIP,
    a.DeviceID AS SharedDevice
FROM InsuranceFraudDB.Bronze.DeviceFingerprints a
JOIN InsuranceFraudDB.Bronze.DeviceFingerprints b ON a.IPAddress = b.IPAddress OR a.DeviceID = b.DeviceID
WHERE a.ClaimID <> b.ClaimID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Fraud Rings
-------------------------------------------------------------------------------
-- Description: Aggregating links to find clusters.
-- Logic: Claims with > 2 links to other claims are suspicious.

CREATE OR REPLACE VIEW InsuranceFraudDB.Gold.FraudRingSuspects AS
SELECT
    ClaimA AS SuspiciousClaimID,
    COUNT(DISTINCT ClaimB) AS LinkedClaimCount,
    MAX(SharedIP) AS LinkedIP,
    MAX(SharedDevice) AS LinkedDevice
FROM InsuranceFraudDB.Silver.LinkedClaims
GROUP BY ClaimA
HAVING COUNT(DISTINCT ClaimB) > 1;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all SuspiciousClaimID from InsuranceFraudDB.Gold.FraudRingSuspects with LinkedClaimCount greater than 2."

PROMPT:
"Show the full claim details for the suspicious claims identified in the FraudRingSuspects view."
*/
