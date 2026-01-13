/*
 * Reinsurance Catastrophe Modeling Demo
 * 
 * Scenario:
 * A reinsurance firm evaluating exposure to natural disasters (Hurricanes, Earthquakes).
 * 
 * Data Context:
 * - Policies: Insured properties with location and coverage limits.
 * - HazardZones: Geospatial risk areas (e.g., Florida Coast).
 * 
 * Analytical Goal:
 * Calculate "Probable Maximum Loss" (PML) for a specific Zone (e.g., 'Zone A - Hurricane').
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ReinsuranceRiskDB;
CREATE FOLDER IF NOT EXISTS ReinsuranceRiskDB.Bronze;
CREATE FOLDER IF NOT EXISTS ReinsuranceRiskDB.Silver;
CREATE FOLDER IF NOT EXISTS ReinsuranceRiskDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ReinsuranceRiskDB.Bronze.Policies (
    PolicyID VARCHAR,
    PropertyAddress VARCHAR,
    ZipCode VARCHAR,
    TIV DOUBLE, -- Total Insured Value
    Deductible DOUBLE
);

CREATE TABLE IF NOT EXISTS ReinsuranceRiskDB.Bronze.HazardEvents (
    EventID VARCHAR,
    EventType VARCHAR, -- 'Hurricane', 'Quake'
    ImpactZipCodes VARCHAR, -- Comma sep list or prefix
    SeverityFactor DOUBLE -- 0.1 to 1.0 (Damage Ratio)
);

INSERT INTO ReinsuranceRiskDB.Bronze.Policies VALUES
('P-001', '123 Ocean Dr', '33101', 5000000.0, 50000.0), -- Miami
('P-002', '456 Beach Rd', '33102', 2000000.0, 20000.0),
('P-003', '789 Inland Wy', '32000', 1000000.0, 10000.0), -- Orlando
('P-004', '101 Coast Blvd', '33101', 10000000.0, 100000.0),
('P-005', '202 Bay St', '33102', 3000000.0, 30000.0),
('P-006', '303 River Ln', '33101', 1500000.0, 15000.0),
('P-007', '404 Palm Ave', '33103', 2500000.0, 25000.0),
('P-008', '505 Dune Rd', '33101', 4000000.0, 40000.0),
('P-009', '606 Sand St', '32001', 800000.0, 8000.0),
('P-010', '707 Shell Ct', '33103', 1200000.0, 12000.0);

INSERT INTO ReinsuranceRiskDB.Bronze.HazardEvents VALUES
('CAT-5-Ian', 'Hurricane', '33101,33102,33103', 0.50); -- 50% damage in these zips

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ReinsuranceRiskDB.Silver.ExposureAnalysis AS
SELECT 
    p.PolicyID,
    p.ZipCode,
    p.TIV,
    p.Deductible,
    h.EventID,
    h.SeverityFactor,
    -- Est Loss = (TIV * Severity) - Deductible
    GREATEST(0, (p.TIV * h.SeverityFactor) - p.Deductible) AS EstimatedClaim
FROM ReinsuranceRiskDB.Bronze.Policies p
CROSS JOIN ReinsuranceRiskDB.Bronze.HazardEvents h
WHERE h.ImpactZipCodes LIKE '%' || p.ZipCode || '%';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ReinsuranceRiskDB.Gold.ZonePML AS
SELECT 
    EventID,
    COUNT(PolicyID) AS ImpactedPolicies,
    SUM(TIV) AS TotalExposureTIV,
    SUM(EstimatedClaim) AS TotalProbableLoss
FROM ReinsuranceRiskDB.Silver.ExposureAnalysis
GROUP BY EventID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the TotalProbableLoss for EventID 'CAT-5-Ian' in ReinsuranceRiskDB.Gold.ZonePML?"
*/
