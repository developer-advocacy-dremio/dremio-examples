/*
 * Government Procurement & Spend Transparency Demo
 * 
 * Scenario:
 * Analyzing government contracts, vendor diversity, and spending against budget.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Procurement;
CREATE FOLDER IF NOT EXISTS RetailDB.Procurement.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Procurement.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Procurement.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Procurement.Bronze.Contracts (
    ContractID INT,
    VendorName VARCHAR,
    Category VARCHAR, -- IT, Construction, Services
    AwardDate DATE,
    ContractAmount DOUBLE,
    VendorType VARCHAR -- SmallBusiness, MinorityOwned, VeteranOwned, Standard
);

INSERT INTO RetailDB.Procurement.Bronze.Contracts VALUES
(1, 'TechSol', 'IT', '2025-01-01', 500000.00, 'SmallBusiness'),
(2, 'BuildCorp', 'Construction', '2025-01-02', 2500000.00, 'Standard'),
(3, 'CleanCo', 'Services', '2025-01-03', 45000.00, 'MinorityOwned'),
(4, 'SecureNet', 'IT', '2025-01-05', 120000.00, 'VeteranOwned'),
(5, 'MegaRoads', 'Construction', '2025-01-10', 5000000.00, 'Standard'),
(6, 'GreenLand', 'Services', '2025-01-15', 30000.00, 'SmallBusiness'),
(7, 'CloudSys', 'IT', '2025-01-20', 800000.00, 'Standard'),
(8, 'OfficeSupply', 'Services', '2025-01-25', 15000.00, 'SmallBusiness'),
(9, 'BridgeMasters', 'Construction', '2025-02-01', 1500000.00, 'Standard'),
(10, 'DataFlow', 'IT', '2025-02-05', 300000.00, 'MinorityOwned'),
(11, 'SafeGuard', 'Services', '2025-02-10', 50000.00, 'VeteranOwned');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Procurement.Silver.SpendAnalysis AS
SELECT 
    VendorType,
    Category,
    ContractAmount,
    CASE 
        WHEN VendorType = 'Standard' THEN 'Unclassified'
        ELSE 'Diversity'
    END AS Classification
FROM RetailDB.Procurement.Bronze.Contracts;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Procurement.Gold.DiversitySpend AS
SELECT 
    Classification,
    COUNT(*) AS ContractCount,
    SUM(ContractAmount) AS TotalSpend,
    (SUM(ContractAmount) * 100.0 / SUM(SUM(ContractAmount)) OVER ()) AS SpendPercentage
FROM RetailDB.Procurement.Silver.SpendAnalysis
GROUP BY Classification;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the total spend on 'Diversity' classified contracts in RetailDB.Procurement.Gold.DiversitySpend?"
*/
