/*
    Dremio High-Volume SQL Pattern: Supply Chain Component Sourcing
    
    Business Scenario:
    Relying on a single supplier for critical components (e.g., Microchips) creates high risk.
    We need to identify "Single Source" components and their geographic concentration.
    
    Data Story:
    - Bronze: BillOfMaterials (BOM), SupplierMap.
    - Silver: ComponentSourcingProfile (Joined counts).
    - Gold: RiskReport (High Risk items).
    
    Medallion Architecture:
    - Bronze: BillOfMaterials, SupplierMap.
      *Volume*: 50+ records.
    - Silver: ComponentSourcingProfile.
    - Gold: RiskReport.
    
    Key Dremio Features:
    - COUNT DISTINCT
    - Geo-spatial grouping (Region)
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SupplyChainDB;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Bronze;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Silver;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Gold;
USE SupplyChainDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SupplyChainDB.Bronze.BillOfMaterials (
    ProductID STRING,
    ComponentID STRING,
    QtyPerUnit INT
);

INSERT INTO SupplyChainDB.Bronze.BillOfMaterials VALUES
('P101', 'C001', 1), ('P101', 'C002', 2), ('P101', 'C003', 1),
('P102', 'C001', 1), ('P102', 'C004', 4), ('P102', 'C005', 1),
('P103', 'C002', 2), ('P103', 'C006', 1), ('P103', 'C007', 1),
('P104', 'C001', 1), ('P104', 'C008', 2), ('P104', 'C009', 1),
('P105', 'C003', 1), ('P105', 'C010', 5), ('P105', 'C011', 1),
('P106', 'C001', 1), ('P106', 'C012', 2), ('P106', 'C013', 1),
('P107', 'C004', 2), ('P107', 'C014', 1), ('P107', 'C015', 1),
('P108', 'C002', 1), ('P108', 'C016', 3), ('P108', 'C017', 1),
('P109', 'C005', 1), ('P109', 'C018', 1), ('P109', 'C019', 1),
('P110', 'C001', 2), ('P110', 'C020', 1); -- 30 BOM Lines

CREATE OR REPLACE TABLE SupplyChainDB.Bronze.SupplierMap (
    ComponentID STRING,
    SupplierID STRING,
    Region STRING, -- APAC, NA, EMEA
    RiskScore INT -- 100 = High Risk
);

INSERT INTO SupplyChainDB.Bronze.SupplierMap VALUES
-- Single Sourced (Risk)
('C001', 'S001', 'APAC', 80), 
('C002', 'S002', 'NA', 20),
('C003', 'S003', 'EMEA', 40),

-- Dual Sourced
('C004', 'S004', 'APAC', 60), ('C004', 'S005', 'NA', 30),
('C005', 'S006', 'EMEA', 50), ('C005', 'S002', 'NA', 20),
('C006', 'S007', 'APAC', 70), ('C006', 'S008', 'APAC', 70), -- Geo Concentration Risk

-- Others
('C007', 'S009', 'NA', 10),
('C008', 'S010', 'EMEA', 30),
('C009', 'S001', 'APAC', 80), -- Single
('C010', 'S002', 'NA', 20),
('C011', 'S003', 'EMEA', 40),
('C012', 'S004', 'APAC', 60),
('C013', 'S005', 'NA', 30),
('C014', 'S006', 'EMEA', 50),
('C015', 'S007', 'APAC', 70),
('C016', 'S009', 'NA', 10), -- Single
('C017', 'S010', 'EMEA', 30),
('C018', 'S001', 'APAC', 80),
('C019', 'S002', 'NA', 20),
('C020', 'S003', 'EMEA', 40);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Component Risk Profile
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SupplyChainDB.Silver.ComponentRiskProfile AS
SELECT
    ComponentID,
    COUNT(DISTINCT SupplierID) AS SupplierCount,
    COUNT(DISTINCT Region) AS RegionCount,
    MAX(RiskScore) AS MaxSupplierRisk,
    LISTAGG(SupplierID, ', ') AS Suppliers
FROM SupplyChainDB.Bronze.SupplierMap
GROUP BY ComponentID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Single Source Risks
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SupplyChainDB.Gold.SingleSourceRisks AS
SELECT
    c.ComponentID,
    c.SupplierCount,
    c.RegionCount,
    c.MaxSupplierRisk,
    b.ProductID,
    CASE 
        WHEN c.SupplierCount = 1 THEN 'CRITICAL_SINGLE_SOURCE'
        WHEN c.RegionCount = 1 THEN 'GEO_CONCENTRATION_RISK'
        ELSE 'DIVERSIFIED'
    END AS RiskCategory
FROM SupplyChainDB.Silver.ComponentRiskProfile c
JOIN SupplyChainDB.Bronze.BillOfMaterials b ON c.ComponentID = b.ComponentID
WHERE c.SupplierCount = 1 OR c.RegionCount = 1;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Count Critical Single Source components by Product."
    2. "Which components are sourced only from 'APAC'?"
    3. "List high-risk suppliers (Score > 75) used in Product P101."
*/
