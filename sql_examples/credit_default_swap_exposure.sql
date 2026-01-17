/*
 * Credit Default Swap (CDS) Exposure Demo
 * 
 * Scenario:
 * The credit trading desk needs to monitor its net exposure to various Reference Entities (companies/sovereigns).
 * CDS contracts act as insurance against default; buying protection = short risk, selling protection = long risk.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Calculate Net Notional (Buy - Sell) per entity and sector.
 * 
 * Note: Assumes a catalog named 'DerivativesDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS DerivativesDB;
CREATE FOLDER IF NOT EXISTS DerivativesDB.Bronze;
CREATE FOLDER IF NOT EXISTS DerivativesDB.Silver;
CREATE FOLDER IF NOT EXISTS DerivativesDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Trade Data
-------------------------------------------------------------------------------
-- Description: Raw CDS trade repository data.

CREATE TABLE IF NOT EXISTS DerivativesDB.Bronze.CDS_Contracts (
    TradeID VARCHAR,
    EntityID VARCHAR,
    Direction VARCHAR, -- 'BuyProtection', 'SellProtection'
    Notional DOUBLE,
    SpreadBps DOUBLE,
    MaturityDate DATE
);

CREATE TABLE IF NOT EXISTS DerivativesDB.Bronze.ReferenceEntities (
    EntityID VARCHAR,
    EntityName VARCHAR,
    Sector VARCHAR, -- 'Energy', 'Tech', 'Sovereign'
    Region VARCHAR
);

-- 1.1 Populate CDS_Contracts (50+ Records)
INSERT INTO DerivativesDB.Bronze.CDS_Contracts (TradeID, EntityID, Direction, Notional, SpreadBps, MaturityDate) VALUES
('CDS-001', 'ENT-01', 'BuyProtection', 5000000, 150, '2026-06-20'),
('CDS-002', 'ENT-01', 'SellProtection', 2000000, 145, '2026-06-20'),
('CDS-003', 'ENT-02', 'BuyProtection', 10000000, 300, '2027-12-20'),
('CDS-004', 'ENT-03', 'SellProtection', 5000000, 50, '2025-09-20'),
('CDS-005', 'ENT-02', 'BuyProtection', 2000000, 310, '2027-12-20'),
('CDS-006', 'ENT-04', 'SellProtection', 7500000, 200, '2026-03-20'),
('CDS-007', 'ENT-01', 'BuyProtection', 1000000, 155, '2026-06-20'),
('CDS-008', 'ENT-05', 'SellProtection', 3000000, 400, '2028-06-20'),
('CDS-009', 'ENT-03', 'BuyProtection', 4000000, 55, '2025-09-20'),
('CDS-010', 'ENT-06', 'SellProtection', 6000000, 90, '2026-12-20'),
('CDS-011', 'ENT-07', 'BuyProtection', 5500000, 120, '2027-06-20'),
('CDS-012', 'ENT-02', 'SellProtection', 8000000, 295, '2027-12-20'),
('CDS-013', 'ENT-08', 'BuyProtection', 2500000, 250, '2026-09-20'),
('CDS-014', 'ENT-04', 'BuyProtection', 1500000, 210, '2026-03-20'),
('CDS-015', 'ENT-01', 'SellProtection', 3500000, 140, '2026-06-20'),
('CDS-016', 'ENT-09', 'SellProtection', 9000000, 30, '2029-12-20'),
('CDS-017', 'ENT-03', 'BuyProtection', 6500000, 60, '2025-09-20'),
('CDS-018', 'ENT-07', 'SellProtection', 4500000, 115, '2027-06-20'),
('CDS-019', 'ENT-05', 'BuyProtection', 3200000, 410, '2028-06-20'),
('CDS-020', 'ENT-10', 'SellProtection', 5200000, 180, '2026-06-20'),
('CDS-021', 'ENT-01', 'BuyProtection', 1200000, 152, '2026-06-20'),
('CDS-022', 'ENT-06', 'BuyProtection', 2200000, 95, '2026-12-20'),
('CDS-023', 'ENT-02', 'BuyProtection', 3300000, 305, '2027-12-20'),
('CDS-024', 'ENT-08', 'SellProtection', 1800000, 245, '2026-09-20'),
('CDS-025', 'ENT-04', 'SellProtection', 2900000, 195, '2026-03-20'),
('CDS-026', 'ENT-09', 'BuyProtection', 4900000, 35, '2029-12-20'),
('CDS-027', 'ENT-05', 'SellProtection', 1100000, 395, '2028-06-20'),
('CDS-028', 'ENT-10', 'BuyProtection', 6100000, 185, '2026-06-20'),
('CDS-029', 'ENT-07', 'BuyProtection', 3800000, 125, '2027-06-20'),
('CDS-030', 'ENT-03', 'SellProtection', 2700000, 45, '2025-09-20'),
('CDS-031', 'ENT-01', 'SellProtection', 4200000, 142, '2026-06-20'),
('CDS-032', 'ENT-06', 'SellProtection', 5800000, 85, '2026-12-20'),
('CDS-033', 'ENT-08', 'BuyProtection', 1600000, 255, '2026-09-20'),
('CDS-034', 'ENT-02', 'SellProtection', 3400000, 290, '2027-12-20'),
('CDS-035', 'ENT-04', 'BuyProtection', 2300000, 205, '2026-03-20'),
('CDS-036', 'ENT-09', 'SellProtection', 7200000, 25, '2029-12-20'),
('CDS-037', 'ENT-05', 'BuyProtection', 1300000, 420, '2028-06-20'),
('CDS-038', 'ENT-10', 'SellProtection', 4300000, 175, '2026-06-20'),
('CDS-039', 'ENT-07', 'SellProtection', 2100000, 110, '2027-06-20'),
('CDS-040', 'ENT-03', 'BuyProtection', 5100000, 65, '2025-09-20'),
('CDS-041', 'ENT-01', 'BuyProtection', 1900000, 158, '2026-06-20'),
('CDS-042', 'ENT-06', 'BuyProtection', 2800000, 98, '2026-12-20'),
('CDS-043', 'ENT-09', 'BuyProtection', 3600000, 38, '2029-12-20'),
('CDS-044', 'ENT-02', 'BuyProtection', 4400000, 315, '2027-12-20'),
('CDS-045', 'ENT-04', 'SellProtection', 1400000, 190, '2026-03-20'),
('CDS-046', 'ENT-08', 'SellProtection', 2600000, 240, '2026-09-20'),
('CDS-047', 'ENT-05', 'SellProtection', 3900000, 390, '2028-06-20'),
('CDS-048', 'ENT-10', 'BuyProtection', 4800000, 190, '2026-06-20'),
('CDS-049', 'ENT-07', 'BuyProtection', 3100000, 130, '2027-06-20'),
('CDS-050', 'ENT-03', 'SellProtection', 5700000, 40, '2025-09-20');

-- 1.2 Populate ReferenceEntities
INSERT INTO DerivativesDB.Bronze.ReferenceEntities (EntityID, EntityName, Sector, Region) VALUES
('ENT-01', 'Acme Energy', 'Energy', 'NA'),
('ENT-02', 'Omega Tech', 'Tech', 'NA'),
('ENT-03', 'Globex Corp', 'Industrial', 'EU'),
('ENT-04', 'Soylent Corp', 'Food', 'APAC'),
('ENT-05', 'Massive Dynamic', 'Tech', 'NA'),
('ENT-06', 'Wayne Enterprises', 'Conglomerate', 'NA'),
('ENT-07', 'Cyberdyne Systems', 'Tech', 'NA'),
('ENT-08', 'Umbrella Corp', 'Pharma', 'EU'),
('ENT-09', 'US Treasury', 'Sovereign', 'NA'),
('ENT-10', 'Stark Industries', 'Defense', 'NA');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Exposure Calculation
-------------------------------------------------------------------------------
-- Description: Combining trade direction to calculate signed notional.
-- Transformation: BuyProtection = Positive Risk (Short Underlying), SellProtection = Negative Risk (Long Underlying).
-- *Note: Conventions vary by desk, here we assume Buy Prot = Risk of counterparty paying out.*

CREATE OR REPLACE VIEW DerivativesDB.Silver.SignedExposure AS
SELECT
    t.TradeID,
    t.EntityID,
    e.EntityName,
    e.Sector,
    t.Notional,
    t.Direction,
    CASE 
        WHEN t.Direction = 'BuyProtection' THEN t.Notional 
        ELSE -t.Notional 
    END AS SignedNotional
FROM DerivativesDB.Bronze.CDS_Contracts t
JOIN DerivativesDB.Bronze.ReferenceEntities e ON t.EntityID = e.EntityID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Aggregated Risk
-------------------------------------------------------------------------------
-- Description: Netting out exposure by Entity and Sector.
-- Insight: Identifying concentration limits breaches.

CREATE OR REPLACE VIEW DerivativesDB.Gold.NetNotionalExposure AS
SELECT
    EntityName,
    Sector,
    SUM(SignedNotional) AS NetNotional,
    COUNT(TradeID) AS TradeCount
FROM DerivativesDB.Silver.SignedExposure
GROUP BY EntityName, Sector;

CREATE OR REPLACE VIEW DerivativesDB.Gold.ConcentrationRiskBySector AS
SELECT
    Sector,
    SUM(ABS(SignedNotional)) AS GrossExposure,
    SUM(SignedNotional) AS NetExposure
FROM DerivativesDB.Silver.SignedExposure
GROUP BY Sector;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Using DerivativesDB.Gold.NetNotionalExposure, create a bar chart showing the NetNotional for each EntityName."

PROMPT:
"Which Sector has the highest GrossExposure in DerivativesDB.Gold.ConcentrationRiskBySector?"
*/
