/*
 * Dremio Luxury Counterfeit Detection Example
 * 
 * Domain: Retail & Blockchain
 * Scenario: 
 * A high-end luxury brand (Handbags, Watches) is fighting a wave of "Superfakes". 
 * They enable all authentic products with NFC chips recorded on a Blockchain Ledger.
 * When a customer or retailer scans the product, the location is logged.
 * 
 * "Impossible Travel" Logic: If the same unique serial number is scanned in Paris 
 * and then Tokyo 5 minutes later, one of them is a "Cloned Tag" (Counterfeit).
 * 
 * Complexity: Medium (Geo-fencing, Self-Join for anomaly detection)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Luxury_Brand;
CREATE FOLDER IF NOT EXISTS Luxury_Brand.Sources;
CREATE FOLDER IF NOT EXISTS Luxury_Brand.Bronze;
CREATE FOLDER IF NOT EXISTS Luxury_Brand.Silver;
CREATE FOLDER IF NOT EXISTS Luxury_Brand.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Luxury_Brand.Sources.Product_Ledger (
    SerialID VARCHAR,
    Product_SKU VARCHAR,
    Manufacture_Date DATE,
    Origin_Factory VARCHAR,
    Authorized_Region VARCHAR, -- 'EU', 'NA', 'APAC'
    Blockchain_Hash VARCHAR
);

CREATE TABLE IF NOT EXISTS Luxury_Brand.Sources.Market_Scans (
    ScanID VARCHAR,
    SerialID VARCHAR,
    Scan_Timestamp TIMESTAMP,
    Scan_Location_Region VARCHAR, -- 'EU', 'NA', 'APAC', 'Unknown'
    RetailerID VARCHAR, -- If scan happened at a store
    Consumer_App_ID VARCHAR -- If scan happened via consumer phone
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Ledger (Authentic Products)
INSERT INTO Luxury_Brand.Sources.Product_Ledger VALUES
('SN-1001', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA1...'),
('SN-1002', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA2...'),
('SN-1003', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA3...'),
('SN-1004', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA4...'),
('SN-1005', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA5...'),
('SN-1006', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA6...'),
('SN-1007', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA7...'),
('SN-1008', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA8...'),
('SN-1009', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxA9...'),
('SN-1010', 'BAG-Classic', '2023-01-01', 'Milan-01', 'EU', 'oxB1...'),
('SN-2001', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC1...'),
('SN-2002', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC2...'),
('SN-2003', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC3...'),
('SN-2004', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC4...'),
('SN-2005', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC5...'),
('SN-2006', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC6...'),
('SN-2007', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC7...'),
('SN-2008', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC8...'),
('SN-2009', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxC9...'),
('SN-2010', 'WATCH-Divers', '2023-02-01', 'Geneva-01', 'NA', 'oxD1...'),
('SN-3001', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE1...'),
('SN-3002', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE2...'),
('SN-3003', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE3...'),
('SN-3004', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE4...'),
('SN-3005', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE5...'),
('SN-3006', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE6...'),
('SN-3007', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE7...'),
('SN-3008', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE8...'),
('SN-3009', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxE9...'),
('SN-3010', 'SCARF-Silk',   '2023-03-01', 'Lyon-01',   'APAC', 'oxF1...'),
('SN-4001', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG1...'),
('SN-4002', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG2...'),
('SN-4003', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG3...'),
('SN-4004', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG4...'),
('SN-4005', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG5...'),
('SN-4006', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG6...'),
('SN-4007', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG7...'),
('SN-4008', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG8...'),
('SN-4009', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxG9...'),
('SN-4010', 'SHOE-Pump',    '2023-04-01', 'Florence-02', 'EU', 'oxH1...'),
('SN-5001', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI1...'),
('SN-5002', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI2...'),
('SN-5003', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI3...'),
('SN-5004', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI4...'),
('SN-5005', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI5...'),
('SN-5006', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI6...'),
('SN-5007', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI7...'),
('SN-5008', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI8...'),
('SN-5009', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxI9...'),
('SN-5010', 'WALLET-Leather', '2023-05-01', 'Madrid-01', 'NA', 'oxJ1...');

-- Seed Scans
INSERT INTO Luxury_Brand.Sources.Market_Scans VALUES
('SCN-001', 'SN-1001', '2023-06-01 10:00:00', 'EU', 'Store-Paris', NULL), -- Valid
('SCN-002', 'SN-1002', '2023-06-01 11:00:00', 'EU', 'Store-London', NULL),
('SCN-003', 'SN-1003', '2023-06-01 12:00:00', 'NA', 'Store-NYC', NULL), -- Grey Market (EU bag in NA)
('SCN-004', 'SN-1004', '2023-06-01 13:00:00', 'EU', NULL, 'App-User1'),
('SCN-005', 'SN-1005', '2023-06-01 14:00:00', 'EU', NULL, 'App-User2'),
('SCN-006', 'SN-1006', '2023-06-01 15:00:00', 'APAC', 'Store-Tokyo', NULL), -- Grey Market
('SCN-007', 'SN-1001', '2023-06-02 10:00:00', 'EU', NULL, 'App-User3'), -- Re-scan of 1001, OK
('SCN-008', 'SN-1001', '2023-06-02 12:00:00', 'NA', NULL, 'App-User4'), -- Re-scan in NA? Suspicious movement
('SCN-009', 'SN-9999', '2023-06-01 10:00:00', 'EU', 'Store-Paris', NULL), -- Fake Serial (Not in ledger)
('SCN-010', 'SN-1001', '2023-06-01 10:05:00', 'EU', 'Store-Rome', NULL), -- Duplicate scan! 1001 scanned in Paris at 10:00, Rome at 10:05. Impossible travel. CLONED TAG.
('SCN-011', 'SN-2001', '2023-06-05 10:00:00', 'NA', 'Store-LA', NULL),
('SCN-012', 'SN-2002', '2023-06-05 11:00:00', 'NA', 'Store-Miami', NULL),
('SCN-013', 'SN-2003', '2023-06-05 12:00:00', 'EU', 'Store-Berlin', NULL), -- Grey
('SCN-014', 'SN-3001', '2023-06-10 10:00:00', 'APAC', 'Store-Seoul', NULL),
('SCN-015', 'SN-3002', '2023-06-10 11:00:00', 'APAC', 'Store-Shanghai', NULL),
('SCN-016', 'SN-3003', '2023-06-10 12:00:00', 'NA', 'Store-Toronto', NULL), -- Grey
('SCN-017', 'SN-4001', '2023-06-15 10:00:00', 'EU', 'Store-Milan', NULL),
('SCN-018', 'SN-4002', '2023-06-15 11:00:00', 'EU', 'Store-Madrid', NULL),
('SCN-019', 'SN-4003', '2023-06-15 12:00:00', 'APAC', 'Store-Sydney', NULL), -- Grey
('SCN-020', 'SN-5001', '2023-06-20 10:00:00', 'NA', 'Store-Chicago', NULL),
('SCN-021', 'SN-5002', '2023-06-20 11:00:00', 'NA', 'Store-Dallas', NULL),
('SCN-022', 'SN-5003', '2023-06-20 12:00:00', 'EU', 'Store-Lisbon', NULL), -- Grey
('SCN-023', 'SN-5003', '2023-06-20 12:05:00', 'NA', 'Store-Dallas', NULL), -- Clone check: Lisbon vs Dallas in 5 mins
('SCN-024', 'SN-1002', '2023-06-03 10:00:00', 'EU', NULL, 'App-User5'),
('SCN-025', 'SN-1002', '2023-06-03 10:01:00', 'EU', NULL, 'App-User5'), -- Double tap, OK
('SCN-026', 'SN-1007', '2023-06-04 10:00:00', 'EU', 'Store-Paris', NULL),
('SCN-027', 'SN-1008', '2023-06-04 10:00:00', 'EU', 'Store-Paris', NULL),
('SCN-028', 'SN-1009', '2023-06-04 10:00:00', 'EU', 'Store-Paris', NULL),
('SCN-029', 'SN-1010', '2023-06-04 10:00:00', 'EU', 'Store-Paris', NULL),
('SCN-030', 'SN-2004', '2023-06-05 10:00:00', 'NA', 'Store-LA', NULL),
('SCN-031', 'SN-2005', '2023-06-05 10:00:00', 'NA', 'Store-LA', NULL),
('SCN-032', 'SN-2006', '2023-06-05 10:00:00', 'NA', 'Store-LA', NULL),
('SCN-033', 'SN-2007', '2023-06-05 10:00:00', 'NA', 'Store-LA', NULL),
('SCN-034', 'SN-3004', '2023-06-10 10:00:00', 'APAC', 'Store-Seoul', NULL),
('SCN-035', 'SN-3005', '2023-06-10 10:00:00', 'APAC', 'Store-Seoul', NULL),
('SCN-036', 'SN-3006', '2023-06-10 10:00:00', 'APAC', 'Store-Seoul', NULL),
('SCN-037', 'SN-3007', '2023-06-10 10:00:00', 'APAC', 'Store-Seoul', NULL),
('SCN-038', 'SN-4004', '2023-06-15 10:00:00', 'EU', 'Store-Milan', NULL),
('SCN-039', 'SN-4005', '2023-06-15 10:00:00', 'EU', 'Store-Milan', NULL),
('SCN-040', 'SN-4006', '2023-06-15 10:00:00', 'EU', 'Store-Milan', NULL),
('SCN-041', 'SN-4007', '2023-06-15 10:00:00', 'EU', 'Store-Milan', NULL),
('SCN-042', 'SN-5004', '2023-06-20 10:00:00', 'NA', 'Store-Chicago', NULL),
('SCN-043', 'SN-5005', '2023-06-20 10:00:00', 'NA', 'Store-Chicago', NULL),
('SCN-044', 'SN-5006', '2023-06-20 10:00:00', 'NA', 'Store-Chicago', NULL),
('SCN-045', 'SN-5007', '2023-06-20 10:00:00', 'NA', 'Store-Chicago', NULL),
('SCN-046', 'SN-1001', '2023-06-01 10:01:00', 'APAC', 'Store-Tokyo', NULL), -- Clone! 10:00 Paris, 10:01 Tokyo
('SCN-047', 'SN-2001', '2023-06-05 10:01:00', 'EU', 'Store-London', NULL), -- Clone!
('SCN-048', 'SN-3001', '2023-06-10 10:01:00', 'NA', 'Store-NYC', NULL), -- Clone!
('SCN-049', 'SN-4001', '2023-06-15 10:01:00', 'APAC', 'Store-Seoul', NULL), -- Clone!
('SCN-050', 'SN-5001', '2023-06-20 10:01:00', 'EU', 'Store-Berlin', NULL); -- Clone!


-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER (Join to see if Serial exists at all)
CREATE OR REPLACE VIEW Luxury_Brand.Bronze.Bronze_Scan_Ledger AS
SELECT
    s.ScanID,
    s.SerialID,
    s.Scan_Timestamp,
    s.Scan_Location_Region,
    l.SerialID as Ledger_Serial,
    l.Authorized_Region
FROM Luxury_Brand.Sources.Market_Scans s
LEFT JOIN Luxury_Brand.Sources.Product_Ledger l ON s.SerialID = l.SerialID;

-- 4b. SILVER LAYER (Validity Checks)
CREATE OR REPLACE VIEW Luxury_Brand.Silver.Silver_Scan_Status AS
SELECT
    ScanID,
    SerialID,
    Scan_Timestamp,
    Scan_Location_Region,
    Authorized_Region,
    CASE
        WHEN Ledger_Serial IS NULL THEN 'Counterfeit - Serial Not Found'
        WHEN Scan_Location_Region != Authorized_Region THEN 'Grey Market Warning'
        ELSE 'Authentic'
    END as Auth_Status
FROM Luxury_Brand.Bronze.Bronze_Scan_Ledger;

-- 4c. GOLD LAYER (Impossible Travel Detection)
-- Self-join to find scans of same serial at diff locations within short time
CREATE OR REPLACE VIEW Luxury_Brand.Gold.Gold_Clone_Alerts AS
SELECT
    s1.SerialID,
    s1.Scan_Location_Region as Loc1,
    s1.Scan_Timestamp as Time1,
    s2.Scan_Location_Region as Loc2,
    s2.Scan_Timestamp as Time2,
    'Cloned Tag Detected' as Alert_Type
FROM Luxury_Brand.Silver.Silver_Scan_Status s1
JOIN Luxury_Brand.Silver.Silver_Scan_Status s2 
  ON s1.SerialID = s2.SerialID 
  AND s1.ScanID != s2.ScanID
WHERE s1.Scan_Location_Region != s2.Scan_Location_Region
AND ABS(EXTRACT(EPOCH FROM (s1.Scan_Timestamp - s2.Scan_Timestamp))) < 3600; -- < 1 Hour diff

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Monitor 'Gold_Clone_Alerts'. If a specific 'Loc1' (e.g., Store-Paris) appears frequently in clone pairs, 
 * investigate typical shipping routes from 'Origin_Factory'."
 */
