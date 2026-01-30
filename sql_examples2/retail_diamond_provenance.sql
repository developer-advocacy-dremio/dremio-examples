/*
 * Retail: Ethical Diamond Provenance Tracking
 * 
 * Scenario:
 * Tracking the blockchain-verified lineage of luxury diamonds from mine to retail to ensure conflict-free status.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS LuxuryProvenanceDB;
CREATE FOLDER IF NOT EXISTS LuxuryProvenanceDB.Jewelry;
CREATE FOLDER IF NOT EXISTS LuxuryProvenanceDB.Jewelry.Bronze;
CREATE FOLDER IF NOT EXISTS LuxuryProvenanceDB.Jewelry.Silver;
CREATE FOLDER IF NOT EXISTS LuxuryProvenanceDB.Jewelry.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Supply Chain Events
-------------------------------------------------------------------------------

-- GemstoneEvents Table
CREATE TABLE IF NOT EXISTS LuxuryProvenanceDB.Jewelry.Bronze.GemstoneEvents (
    GemID VARCHAR,
    EventType VARCHAR, -- Mined, Cut, Certified, Shipped, Sold
    Location VARCHAR,
    KimberleyCombinedCert VARCHAR, -- Gov certification ID
    EventTimestamp TIMESTAMP
);

INSERT INTO LuxuryProvenanceDB.Jewelry.Bronze.GemstoneEvents VALUES
('DIA-001', 'Mined', 'Botswana', 'KPC-1001', '2024-01-01 10:00:00'),
('DIA-001', 'Cut', 'Antwerp', 'KPC-1001', '2024-02-01 14:00:00'),
('DIA-001', 'Certified', 'GIA-Lab-NY', 'GIA-123456', '2024-03-01 09:00:00'),
('DIA-001', 'Sold', 'Retail-NYC', 'INV-5001', '2024-04-01 16:00:00'),
('DIA-002', 'Mined', 'Canada', 'KPC-2200', '2024-01-05 11:00:00'),
('DIA-002', 'Cut', 'Tel Aviv', 'KPC-2200', '2024-02-10 13:00:00'),
('DIA-002', 'Certified', 'AGS-Lab', 'AGS-987654', '2024-03-15 10:00:00'),
('DIA-003', 'Mined', 'Sierra Leone', 'KPC-Unknown', '2024-01-10 08:00:00'), -- Suspicious
('DIA-003', 'Cut', 'Mumbai', 'KPC-Unknown', '2024-02-20 15:00:00'),
('DIA-003', 'Shipped', 'Dubai', 'UNK-001', '2024-03-05 12:00:00'),
('DIA-004', 'Mined', 'Australia', 'KPC-3300', '2024-01-15 09:00:00'),
('DIA-004', 'Cut', 'Mumbai', 'KPC-3300', '2024-02-25 14:00:00'),
('DIA-004', 'Certified', 'GIA-Lab-Mumbai', 'GIA-555555', '2024-03-20 11:00:00'),
('DIA-005', 'Mined', 'Russia', 'KPC-4400', '2024-01-20 10:00:00'), -- Sanction risk
('DIA-005', 'Cut', 'Surat', 'KPC-4400', '2024-02-28 16:00:00'),
('DIA-006', 'Mined', 'Botswana', 'KPC-1002', '2024-01-25 10:00:00'),
('DIA-006', 'Cut', 'Antwerp', 'KPC-1002', '2024-03-01 14:00:00'),
('DIA-007', 'Mined', 'South Africa', 'KPC-5000', '2024-01-30 08:00:00'),
('DIA-007', 'Cut', 'Johannesburg', 'KPC-5000', '2024-03-05 11:00:00'),
('DIA-008', 'Mined', 'Unknown', NULL, '2024-02-01 09:00:00'), -- Red flag
('DIA-008', 'Shipped', 'Hong Kong', NULL, '2024-03-01 12:00:00'),
('DIA-009', 'Mined', 'Canada', 'KPC-2201', '2024-02-05 10:00:00'),
('DIA-009', 'Cut', 'New York', 'KPC-2201', '2024-03-10 15:00:00'),
('DIA-010', 'Mined', 'Botswana', 'KPC-1003', '2024-02-10 11:00:00'),
('DIA-011', 'Mined', 'Namibia', 'KPC-6000', '2024-02-15 09:00:00'),
('DIA-012', 'Mined', 'Angola', 'KPC-7000', '2024-02-20 10:00:00'),
('DIA-013', 'Mined', 'Brazil', 'KPC-8000', '2024-02-25 11:00:00'),
('DIA-014', 'Mined', 'Canada', 'KPC-2202', '2024-03-01 12:00:00'),
('DIA-015', 'Mined', 'Russia', 'KPC-4401', '2024-03-05 09:00:00'),
('DIA-016', 'Mined', 'Botswana', 'KPC-1004', '2024-03-10 10:00:00'),
('DIA-017', 'Mined', 'South Africa', 'KPC-5001', '2024-03-15 11:00:00'),
('DIA-018', 'Mined', 'Australia', 'KPC-3301', '2024-03-20 12:00:00'),
('DIA-019', 'Mined', 'Sierra Leone', 'KPC-9000', '2024-03-25 09:00:00'),
('DIA-020', 'Mined', 'Zimbabwe', 'KPC-9500', '2024-03-30 10:00:00'),
('DIA-021', 'Mined', 'Canada', 'KPC-2203', '2024-04-01 11:00:00'),
('DIA-022', 'Mined', 'Botswana', 'KPC-1005', '2024-04-05 12:00:00'),
('DIA-023', 'Mined', 'Russia', 'KPC-4402', '2024-04-10 09:00:00'), -- Sanction
('DIA-024', 'Mined', 'Namibia', 'KPC-6001', '2024-04-15 10:00:00'),
('DIA-025', 'Mined', 'Angola', 'KPC-7001', '2024-04-20 11:00:00'),
('DIA-026', 'Mined', 'Brazil', 'KPC-8001', '2024-04-25 12:00:00'),
('DIA-027', 'Mined', 'Canada', 'KPC-2204', '2024-05-01 09:00:00'),
('DIA-028', 'Mined', 'Botswana', 'KPC-1006', '2024-05-05 10:00:00'),
('DIA-029', 'Mined', 'South Africa', 'KPC-5002', '2024-05-10 11:00:00'),
('DIA-030', 'Mined', 'Australia', 'KPC-3302', '2024-05-15 12:00:00'),
('DIA-031', 'Mined', 'Sierra Leone', 'KPC-9001', '2024-05-20 09:00:00'),
('DIA-032', 'Mined', 'Zimbabwe', 'KPC-9501', '2024-05-25 10:00:00'),
('DIA-033', 'Mined', 'Canada', 'KPC-2205', '2024-06-01 11:00:00'),
('DIA-034', 'Mined', 'Botswana', 'KPC-1007', '2024-06-05 12:00:00'),
('DIA-035', 'Mined', 'Russia', 'KPC-4403', '2024-06-10 09:00:00'),
('DIA-036', 'Mined', 'Namibia', 'KPC-6002', '2024-06-15 10:00:00'),
('DIA-037', 'Mined', 'Angola', 'KPC-7002', '2024-06-20 11:00:00'),
('DIA-038', 'Mined', 'Brazil', 'KPC-8002', '2024-06-25 12:00:00'),
('DIA-039', 'Mined', 'Canada', 'KPC-2206', '2024-07-01 09:00:00'),
('DIA-040', 'Mined', 'Botswana', 'KPC-1008', '2024-07-05 10:00:00'),
('DIA-041', 'Mined', 'South Africa', 'KPC-5003', '2024-07-10 11:00:00'),
('DIA-042', 'Mined', 'Australia', 'KPC-3303', '2024-07-15 12:00:00'),
('DIA-043', 'Mined', 'Sierra Leone', 'KPC-9002', '2024-07-20 09:00:00'),
('DIA-044', 'Mined', 'Zimbabwe', 'KPC-9502', '2024-07-25 10:00:00'),
('DIA-045', 'Mined', 'Canada', 'KPC-2207', '2024-08-01 11:00:00'),
('DIA-046', 'Mined', 'Botswana', 'KPC-1009', '2024-08-05 12:00:00'),
('DIA-047', 'Mined', 'Russia', 'KPC-4404', '2024-08-10 09:00:00'),
('DIA-048', 'Mined', 'Namibia', 'KPC-6003', '2024-08-15 10:00:00'),
('DIA-049', 'Mined', 'Angola', 'KPC-7003', '2024-08-20 11:00:00'),
('DIA-050', 'Mined', 'Brazil', 'KPC-8003', '2024-08-25 12:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Lineage Construction
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW LuxuryProvenanceDB.Jewelry.Silver.DiamondJourney AS
SELECT 
    GemID,
    LISTAGG(EventType, ' -> ') WITHIN GROUP (ORDER BY EventTimestamp) AS EventChain,
    MIN(CASE WHEN EventType = 'Mined' THEN Location END) AS OriginCountry,
    MIN(CASE WHEN EventType = 'Mined' THEN KimberelyCombinedCert END) AS OriginCert,
    MAX(EventTimestamp) AS LastSeen
FROM LuxuryProvenanceDB.Jewelry.Bronze.GemstoneEvents
GROUP BY GemID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Ethical Compliance Report
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW LuxuryProvenanceDB.Jewelry.Gold.EthicalAudit AS
SELECT 
    GemID,
    OriginCountry,
    OriginCert,
    CASE 
        WHEN OriginCountry IN ('Russia', 'Zimbabwe', 'Unknown') THEN 'High Risk (Sanctions/Conflict)'
        WHEN OriginCert IS NULL OR OriginCert LIKE '%Unknown%' THEN 'Verification Failed'
        ELSE 'Ethical / Conflict-Free'
    END AS ComplianceStatus
FROM LuxuryProvenanceDB.Jewelry.Silver.DiamondJourney;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all diamonds with 'Verification Failed' status in the Gold audit report."

PROMPT 2:
"Trace the full event chain for GemID 'DIA-001' using the Silver layer."

PROMPT 3:
"Count how many diamonds originated from 'Canada' vs 'Russia' in the Gold layer."
*/
