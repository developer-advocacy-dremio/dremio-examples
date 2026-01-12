/*
 * Government Tax Revenue Analysis Demo
 * 
 * Scenario:
 * Analyzing tax collection efficiency, delinquency rates, and revenue projections by district.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Government;
CREATE FOLDER IF NOT EXISTS RetailDB.Government.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Government.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Government.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Government.Bronze.TaxReturns (
    ReturnID INT,
    TaxpayerID INT,
    DistrictCode VARCHAR,
    FilingYear INT,
    DeclaredIncome DOUBLE,
    TaxDue DOUBLE,
    TaxPaid DOUBLE,
    FilingStatus VARCHAR -- OnTime, Late, Delinquent
);

INSERT INTO RetailDB.Government.Bronze.TaxReturns VALUES
(1, 1001, 'D1', 2024, 50000.00, 10000.00, 10000.00, 'OnTime'),
(2, 1002, 'D1', 2024, 75000.00, 15000.00, 15000.00, 'OnTime'),
(3, 1003, 'D2', 2024, 120000.00, 24000.00, 20000.00, 'Late'), -- Partial payment
(4, 1004, 'D2', 2024, 45000.00, 9000.00, 0.00, 'Delinquent'),
(5, 1005, 'D3', 2024, 200000.00, 40000.00, 40000.00, 'OnTime'),
(6, 1006, 'D1', 2024, 60000.00, 12000.00, 12000.00, 'OnTime'),
(7, 1007, 'D3', 2024, 85000.00, 17000.00, 17000.00, 'OnTime'),
(8, 1008, 'D2', 2024, 95000.00, 19000.00, 10000.00, 'Late'),
(9, 1009, 'D1', 2024, 30000.00, 6000.00, 0.00, 'Delinquent'),
(10, 1010, 'D4', 2024, 500000.00, 100000.00, 100000.00, 'OnTime'),
(11, 1011, 'D4', 2024, 55000.00, 11000.00, 11000.00, 'OnTime');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Government.Silver.CollectionMetrics AS
SELECT 
    DistrictCode,
    FilingYear,
    TaxDue,
    TaxPaid,
    (TaxDue - TaxPaid) AS OutstandingBalance,
    CASE 
        WHEN FilingStatus = 'Delinquent' THEN 1 
        ELSE 0 
    END AS IsDelinquent
FROM RetailDB.Government.Bronze.TaxReturns;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Government.Gold.DistrictPerformance AS
SELECT 
    DistrictCode,
    SUM(TaxDue) AS TotalTaxLiability,
    SUM(TaxPaid) AS TotalCollected,
    (SUM(TaxPaid) / SUM(TaxDue)) * 100 AS CollectionRate,
    SUM(IsDelinquent) AS DelinquentCount
FROM RetailDB.Government.Silver.CollectionMetrics
GROUP BY DistrictCode;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which DistrictCode has the lowest CollectionRate in RetailDB.Government.Gold.DistrictPerformance?"
*/
