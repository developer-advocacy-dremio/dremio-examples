/*
    Dremio High-Volume SQL Pattern: Government Tax Revenue Analysis
    
    Business Scenario:
    Analyzing tax collection efficiency, delinquency rates, and revenue projections by district.
    Identifying "High Delinquency" zones for targeted outreach.
    
    Data Story:
    We track Tax Returns and Payments.
    
    Medallion Architecture:
    - Bronze: TaxReturns.
      *Volume*: 50+ records.
    - Silver: CollectionMetrics (Outstanding Balance).
    - Gold: DistrictPerformance (Collection Rate %).
    
    Key Dremio Features:
    - Mathematical Operators
    - Group By
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentTaxDB;
CREATE FOLDER IF NOT EXISTS GovernmentTaxDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentTaxDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentTaxDB.Gold;
USE GovernmentTaxDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentTaxDB.Bronze.TaxReturns (
    ReturnID STRING,
    TaxpayerID STRING,
    DistrictCode STRING,
    FilingYear INT,
    DeclaredIncome DOUBLE,
    TaxDue DOUBLE,
    TaxPaid DOUBLE,
    FilingStatus STRING -- OnTime, Late, Delinquent
);

INSERT INTO GovernmentTaxDB.Bronze.TaxReturns VALUES
('RET001', 'TP1001', 'D1', 2024, 50000.00, 10000.00, 10000.00, 'OnTime'),
('RET002', 'TP1002', 'D1', 2024, 75000.00, 15000.00, 15000.00, 'OnTime'),
('RET003', 'TP1003', 'D2', 2024, 120000.00, 24000.00, 20000.00, 'Late'), -- Partial
('RET004', 'TP1004', 'D2', 2024, 45000.00, 9000.00, 0.00, 'Delinquent'),
('RET005', 'TP1005', 'D3', 2024, 200000.00, 40000.00, 40000.00, 'OnTime'),
('RET006', 'TP1006', 'D1', 2024, 60000.00, 12000.00, 12000.00, 'OnTime'),
('RET007', 'TP1007', 'D3', 2024, 85000.00, 17000.00, 17000.00, 'OnTime'),
('RET008', 'TP1008', 'D2', 2024, 95000.00, 19000.00, 10000.00, 'Late'),
('RET009', 'TP1009', 'D1', 2024, 30000.00, 6000.00, 0.00, 'Delinquent'),
('RET010', 'TP1010', 'D4', 2024, 500000.00, 100000.00, 100000.00, 'OnTime'),
('RET011', 'TP1011', 'D4', 2024, 55000.00, 11000.00, 11000.00, 'OnTime'),
('RET012', 'TP1012', 'D1', 2024, 40000.00, 8000.00, 8000.00, 'OnTime'),
('RET013', 'TP1013', 'D2', 2024, 35000.00, 7000.00, 0.00, 'Delinquent'),
('RET014', 'TP1014', 'D3', 2024, 150000.00, 30000.00, 30000.00, 'OnTime'),
('RET015', 'TP1015', 'D4', 2024, 250000.00, 50000.00, 50000.00, 'OnTime'),
('RET016', 'TP1016', 'D1', 2024, 65000.00, 13000.00, 13000.00, 'Late'),
('RET017', 'TP1017', 'D2', 2024, 55000.00, 11000.00, 5000.00, 'Late'),
('RET018', 'TP1018', 'D3', 2024, 90000.00, 18000.00, 18000.00, 'OnTime'),
('RET019', 'TP1019', 'D4', 2024, 450000.00, 90000.00, 90000.00, 'OnTime'),
('RET020', 'TP1020', 'D1', 2024, 32000.00, 6400.00, 0.00, 'Delinquent'),
('RET021', 'TP1021', 'D2', 2024, 42000.00, 8400.00, 8400.00, 'OnTime'),
('RET022', 'TP1022', 'D3', 2024, 110000.00, 22000.00, 22000.00, 'Late'),
('RET023', 'TP1023', 'D4', 2024, 300000.00, 60000.00, 60000.00, 'OnTime'),
('RET024', 'TP1024', 'D1', 2024, 28000.00, 5600.00, 0.00, 'Delinquent'),
('RET025', 'TP1025', 'D2', 2024, 72000.00, 14400.00, 10000.00, 'Late'),
('RET026', 'TP1026', 'D3', 2024, 88000.00, 17600.00, 17600.00, 'OnTime'),
('RET027', 'TP1027', 'D4', 2024, 350000.00, 70000.00, 70000.00, 'OnTime'),
('RET028', 'TP1028', 'D1', 2024, 48000.00, 9600.00, 9600.00, 'OnTime'),
('RET029', 'TP1029', 'D2', 2024, 58000.00, 11600.00, 0.00, 'Delinquent'),
('RET030', 'TP1030', 'D3', 2024, 130000.00, 26000.00, 26000.00, 'OnTime'),
('RET031', 'TP1031', 'D4', 2024, 400000.00, 80000.00, 80000.00, 'OnTime'),
('RET032', 'TP1032', 'D1', 2024, 36000.00, 7200.00, 7200.00, 'Late'),
('RET033', 'TP1033', 'D2', 2024, 62000.00, 12400.00, 5000.00, 'Late'),
('RET034', 'TP1034', 'D3', 2024, 95000.00, 19000.00, 19000.00, 'OnTime'),
('RET035', 'TP1035', 'D4', 2024, 420000.00, 84000.00, 84000.00, 'OnTime'),
('RET036', 'TP1036', 'D1', 2024, 52000.00, 10400.00, 0.00, 'Delinquent'),
('RET037', 'TP1037', 'D2', 2024, 68000.00, 13600.00, 13600.00, 'OnTime'),
('RET038', 'TP1038', 'D3', 2024, 140000.00, 28000.00, 28000.00, 'OnTime'),
('RET039', 'TP1039', 'D4', 2024, 280000.00, 56000.00, 56000.00, 'OnTime'),
('RET040', 'TP1040', 'D1', 2024, 33000.00, 6600.00, 0.00, 'Delinquent'),
('RET041', 'TP1041', 'D2', 2024, 76000.00, 15200.00, 10000.00, 'Late'),
('RET042', 'TP1042', 'D3', 2024, 98000.00, 19600.00, 19600.00, 'OnTime'),
('RET043', 'TP1043', 'D4', 2024, 320000.00, 64000.00, 64000.00, 'OnTime'),
('RET044', 'TP1044', 'D1', 2024, 46000.00, 9200.00, 9200.00, 'OnTime'),
('RET045', 'TP1045', 'D2', 2024, 54000.00, 10800.00, 0.00, 'Delinquent'),
('RET046', 'TP1046', 'D3', 2024, 115000.00, 23000.00, 23000.00, 'OnTime'),
('RET047', 'TP1047', 'D4', 2024, 380000.00, 76000.00, 76000.00, 'OnTime'),
('RET048', 'TP1048', 'D1', 2024, 29000.00, 5800.00, 0.00, 'Delinquent'),
('RET049', 'TP1049', 'D2', 2024, 80000.00, 16000.00, 16000.00, 'OnTime'),
('RET050', 'TP1050', 'D3', 2024, 125000.00, 25000.00, 25000.00, 'OnTime');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Collection Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentTaxDB.Silver.CollectionMetrics AS
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
FROM GovernmentTaxDB.Bronze.TaxReturns;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: District Performance
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentTaxDB.Gold.DistrictPerformance AS
SELECT 
    DistrictCode,
    SUM(TaxDue) AS TotalTaxLiability,
    SUM(TaxPaid) AS TotalCollected,
    (SUM(TaxPaid) / SUM(TaxDue)) * 100 AS CollectionRate,
    SUM(IsDelinquent) AS DelinquentCount
FROM GovernmentTaxDB.Silver.CollectionMetrics
GROUP BY DistrictCode;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which DistrictCode has the lowest CollectionRate?"
    2. "List delinquencies by District."
    3. "Show Total Collected revenue by District."
*/
