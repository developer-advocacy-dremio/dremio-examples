/*
    Dremio High-Volume SQL Pattern: Government Procurement & Spend Transparency
    
    Business Scenario:
    Analyzing government contracts, vendor diversity, and spending against budget.
    Ensuring compliance with "MinorityOwned" and "SmallBusiness" quotas.
    
    Data Story:
    We track Contract Awards and Vendor Categories.
    
    Medallion Architecture:
    - Bronze: Contracts.
      *Volume*: 50+ records.
    - Silver: SpendAnalysis (Classification).
    - Gold: DiversitySpend (Spend % by Category).
    
    Key Dremio Features:
    - Window Functions (SUM OVER)
    - Case Logic
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentProcurementDB;
CREATE FOLDER IF NOT EXISTS GovernmentProcurementDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentProcurementDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentProcurementDB.Gold;
USE GovernmentProcurementDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentProcurementDB.Bronze.Contracts (
    ContractID STRING,
    VendorName STRING,
    Category STRING, -- IT, Construction, Services
    AwardDate DATE,
    ContractAmount DOUBLE,
    VendorType STRING -- SmallBusiness, MinorityOwned, VeteranOwned, Standard
);

INSERT INTO GovernmentProcurementDB.Bronze.Contracts VALUES
('C1', 'TechSol', 'IT', DATE '2025-01-01', 500000.00, 'SmallBusiness'),
('C2', 'BuildCorp', 'Construction', DATE '2025-01-02', 2500000.00, 'Standard'),
('C3', 'CleanCo', 'Services', DATE '2025-01-03', 45000.00, 'MinorityOwned'),
('C4', 'SecureNet', 'IT', DATE '2025-01-05', 120000.00, 'VeteranOwned'),
('C5', 'MegaRoads', 'Construction', DATE '2025-01-10', 5000000.00, 'Standard'),
('C6', 'GreenLand', 'Services', DATE '2025-01-15', 30000.00, 'SmallBusiness'),
('C7', 'CloudSys', 'IT', DATE '2025-01-20', 800000.00, 'Standard'),
('C8', 'OfficeSupply', 'Services', DATE '2025-01-25', 15000.00, 'SmallBusiness'),
('C9', 'BridgeMasters', 'Construction', DATE '2025-02-01', 1500000.00, 'Standard'),
('C10', 'DataFlow', 'IT', DATE '2025-02-05', 300000.00, 'MinorityOwned'),
('C11', 'SafeGuard', 'Services', DATE '2025-02-10', 50000.00, 'VeteranOwned'),
('C12', 'CityBuilders', 'Construction', DATE '2025-01-01', 2000000.00, 'Standard'),
('C13', 'NetWorks', 'IT', DATE '2025-01-03', 150000.00, 'SmallBusiness'),
('C14', 'EcoClean', 'Services', DATE '2025-01-06', 25000.00, 'MinorityOwned'),
('C15', 'VetForce', 'Services', DATE '2025-01-08', 60000.00, 'VeteranOwned'),
('C16', 'RoadWorks', 'Construction', DATE '2025-01-12', 3000000.00, 'Standard'),
('C17', 'SoftServ', 'IT', DATE '2025-01-14', 400000.00, 'Standard'),
('C18', 'PaperCo', 'Services', DATE '2025-01-18', 10000.00, 'SmallBusiness'),
('C19', 'SteelStructures', 'Construction', DATE '2025-01-22', 1200000.00, 'Standard'),
('C20', 'CodeCrafters', 'IT', DATE '2025-01-26', 250000.00, 'MinorityOwned'),
('C21', 'AlphaSecurity', 'Services', DATE '2025-01-28', 75000.00, 'VeteranOwned'),
('C22', 'ConcretePros', 'Construction', DATE '2025-01-05', 800000.00, 'Standard'),
('C23', 'WebWizards', 'IT', DATE '2025-01-07', 100000.00, 'SmallBusiness'),
('C24', 'JanitorialPlus', 'Services', DATE '2025-01-09', 20000.00, 'MinorityOwned'),
('C25', 'PatriotServices', 'Services', DATE '2025-01-11', 40000.00, 'VeteranOwned'),
('C26', 'PaveIt', 'Construction', DATE '2025-01-13', 4000000.00, 'Standard'),
('C27', 'ServerFarm', 'IT', DATE '2025-01-16', 700000.00, 'Standard'),
('C28', 'PrintShop', 'Services', DATE '2025-01-19', 12000.00, 'SmallBusiness'),
('C29', 'IronWorks', 'Construction', DATE '2025-01-21', 1800000.00, 'Standard'),
('C30', 'AppDev', 'IT', DATE '2025-01-24', 220000.00, 'MinorityOwned'),
('C31', 'GuardDogs', 'Services', DATE '2025-01-27', 35000.00, 'VeteranOwned'),
('C32', 'SkyHigh', 'Construction', DATE '2025-01-02', 2200000.00, 'Standard'),
('C33', 'ByteLogic', 'IT', DATE '2025-01-04', 180000.00, 'SmallBusiness'),
('C34', 'GreenScapes', 'Services', DATE '2025-01-06', 28000.00, 'MinorityOwned'),
('C35', 'MarineOps', 'Services', DATE '2025-01-08', 55000.00, 'VeteranOwned'),
('C36', 'BuildRight', 'Construction', DATE '2025-01-12', 3500000.00, 'Standard'),
('C37', 'CloudHost', 'IT', DATE '2025-01-15', 500000.00, 'Standard'),
('C38', 'CopyCat', 'Services', DATE '2025-01-17', 18000.00, 'SmallBusiness'),
('C39', 'MegaSteel', 'Construction', DATE '2025-01-20', 1600000.00, 'Standard'),
('C40', 'DevOpsInc', 'IT', DATE '2025-01-23', 280000.00, 'MinorityOwned'),
('C41', 'RiskMgmt', 'Services', DATE '2025-01-25', 65000.00, 'VeteranOwned'),
('C42', 'CityPaving', 'Construction', DATE '2025-01-05', 900000.00, 'Standard'),
('C43', 'NetSolutions', 'IT', DATE '2025-01-07', 110000.00, 'SmallBusiness'),
('C44', 'CleanSweep', 'Services', DATE '2025-01-09', 22000.00, 'MinorityOwned'),
('C45', 'VetSecurity', 'Services', DATE '2025-01-11', 45000.00, 'VeteranOwned'),
('C46', 'ConcreteKing', 'Construction', DATE '2025-01-13', 4200000.00, 'Standard'),
('C47', 'DataCenter', 'IT', DATE '2025-01-16', 750000.00, 'Standard'),
('C48', 'Supplies4U', 'Services', DATE '2025-01-19', 14000.00, 'SmallBusiness'),
('C49', 'SteelCity', 'Construction', DATE '2025-01-21', 1900000.00, 'Standard'),
('C50', 'AgileSoft', 'IT', DATE '2025-01-24', 240000.00, 'MinorityOwned');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Spend Classification
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentProcurementDB.Silver.SpendAnalysis AS
SELECT 
    VendorType,
    Category,
    ContractAmount,
    CASE 
        WHEN VendorType = 'Standard' THEN 'Unclassified'
        ELSE 'Diversity'
    END AS Classification
FROM GovernmentProcurementDB.Bronze.Contracts;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Diversity Spend %
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentProcurementDB.Gold.DiversitySpend AS
SELECT 
    Classification,
    COUNT(*) AS ContractCount,
    SUM(ContractAmount) AS TotalSpend,
    (SUM(ContractAmount) * 100.0 / SUM(SUM(ContractAmount)) OVER ()) AS SpendPercentage
FROM GovernmentProcurementDB.Silver.SpendAnalysis
GROUP BY Classification;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "What is the total and percentage spend on Diversity contracts?"
    2. "List the top categories for MinorityOwned vendors."
    3. "Show Contract Amount by Vendor Type."
*/
