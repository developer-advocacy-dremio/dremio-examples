/*
    Dremio High-Volume SQL Pattern: Government Economic Development
    
    Business Scenario:
    Economic Development Corporations (EDCs) track "Job Creation" and "Business Growth" 
    to validate tax incentives and zone performance.
    
    Data Story:
    We track Business Registrations and Employment Reports.
    
    Medallion Architecture:
    - Bronze: BusinessRegistry, QtrlyEmployment.
      *Volume*: 50+ records.
    - Silver: GrowthMetrics (Net Job Change).
    - Gold: SectorReport (Growth by Industry).
    
    Key Dremio Features:
    - Group By Industry
    - Delta Calculations
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentEconDB;
CREATE FOLDER IF NOT EXISTS GovernmentEconDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentEconDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentEconDB.Gold;
USE GovernmentEconDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentEconDB.Bronze.BusinessRegistry (
    BusinessID STRING,
    Name STRING,
    Industry STRING, -- Tech, Mfg, Retail
    ZoneID STRING,
    RegistrationDate DATE
);

INSERT INTO GovernmentEconDB.Bronze.BusinessRegistry VALUES
('BIZ101', 'Tech Nova', 'Tech', 'Zone_A', DATE '2020-01-01'),
('BIZ102', 'FabWorks', 'Mfg', 'Zone_B', DATE '2019-05-15'),
('BIZ103', 'ShopMart', 'Retail', 'Zone_A', DATE '2018-11-20'),
('BIZ104', 'CodeStream', 'Tech', 'Zone_A', DATE '2021-03-10'),
('BIZ105', 'SteelCo', 'Mfg', 'Zone_B', DATE '2015-06-30'),
('BIZ106', 'GreenGrocer', 'Retail', 'Zone_C', DATE '2022-01-05'),
('BIZ107', 'DataFlow', 'Tech', 'Zone_B', DATE '2023-08-15'),
('BIZ108', 'AutoParts', 'Mfg', 'Zone_A', DATE '2010-09-01'),
('BIZ109', 'FashionAve', 'Retail', 'Zone_B', DATE '2021-12-12'),
('BIZ110', 'CloudNine', 'Tech', 'Zone_C', DATE '2024-02-28');


CREATE OR REPLACE TABLE GovernmentEconDB.Bronze.EmploymentReports (
    ReportID STRING,
    BusinessID STRING,
    Quarter STRING, -- Q1, Q2
    Year INT,
    EmployeeCount INT
);

-- Bulk Employment (50+ Records)
INSERT INTO GovernmentEconDB.Bronze.EmploymentReports VALUES
('RPT5001', 'BIZ101', 'Q3', 2024, 150),
('RPT5002', 'BIZ101', 'Q4', 2024, 160), -- Growth
('RPT5003', 'BIZ102', 'Q3', 2024, 80),
('RPT5004', 'BIZ102', 'Q4', 2024, 75), -- Decline
('RPT5005', 'BIZ103', 'Q3', 2024, 40),
('RPT5006', 'BIZ103', 'Q4', 2024, 42),
('RPT5007', 'BIZ104', 'Q3', 2024, 200),
('RPT5008', 'BIZ104', 'Q4', 2024, 210),
('RPT5009', 'BIZ105', 'Q3', 2024, 300),
('RPT5010', 'BIZ105', 'Q4', 2024, 295),
('RPT5011', 'BIZ106', 'Q3', 2024, 15),
('RPT5012', 'BIZ106', 'Q4', 2024, 18),
('RPT5013', 'BIZ107', 'Q3', 2024, 50),
('RPT5014', 'BIZ107', 'Q4', 2024, 60), -- High Growth
('RPT5015', 'BIZ108', 'Q3', 2024, 120),
('RPT5016', 'BIZ108', 'Q4', 2024, 120),
('RPT5017', 'BIZ109', 'Q3', 2024, 25),
('RPT5018', 'BIZ109', 'Q4', 2024, 22),
('RPT5019', 'BIZ110', 'Q3', 2024, 10),
('RPT5020', 'BIZ110', 'Q4', 2024, 15),
('RPT5021', 'BIZ101', 'Q2', 2024, 140),
('RPT5022', 'BIZ101', 'Q1', 2024, 130),
('RPT5023', 'BIZ102', 'Q2', 2024, 82),
('RPT5024', 'BIZ102', 'Q1', 2024, 85),
('RPT5025', 'BIZ103', 'Q2', 2024, 38),
('RPT5026', 'BIZ103', 'Q1', 2024, 35),
('RPT5027', 'BIZ104', 'Q2', 2024, 190),
('RPT5028', 'BIZ104', 'Q1', 2024, 180),
('RPT5029', 'BIZ105', 'Q2', 2024, 305),
('RPT5030', 'BIZ105', 'Q1', 2024, 310),
('RPT5031', 'BIZ106', 'Q2', 2024, 12),
('RPT5032', 'BIZ106', 'Q1', 2024, 10),
('RPT5033', 'BIZ107', 'Q2', 2024, 40),
('RPT5034', 'BIZ107', 'Q1', 2024, 30),
('RPT5035', 'BIZ108', 'Q2', 2024, 118),
('RPT5036', 'BIZ108', 'Q1', 2024, 115),
('RPT5037', 'BIZ109', 'Q2', 2024, 28),
('RPT5038', 'BIZ109', 'Q1', 2024, 30),
('RPT5039', 'BIZ110', 'Q2', 2024, 8),
('RPT5040', 'BIZ110', 'Q1', 2024, 5),
('RPT5041', 'BIZ101', 'Q4', 2023, 120),
('RPT5042', 'BIZ102', 'Q4', 2023, 85),
('RPT5043', 'BIZ103', 'Q4', 2023, 30),
('RPT5044', 'BIZ104', 'Q4', 2023, 150),
('RPT5045', 'BIZ105', 'Q4', 2023, 315),
('RPT5046', 'BIZ106', 'Q4', 2023, 8),
('RPT5047', 'BIZ107', 'Q4', 2023, 20),
('RPT5048', 'BIZ108', 'Q4', 2023, 110),
('RPT5049', 'BIZ109', 'Q4', 2023, 32),
('RPT5050', 'BIZ110', 'Q4', 2023, 0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Job Count
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentEconDB.Silver.JobStats AS
SELECT
    b.Industry,
    b.ZoneID,
    e.Quarter,
    e.Year,
    SUM(e.EmployeeCount) AS TotalJobs
FROM GovernmentEconDB.Bronze.EmploymentReports e
JOIN GovernmentEconDB.Bronze.BusinessRegistry b ON e.BusinessID = b.BusinessID
GROUP BY b.Industry, b.ZoneID, e.Quarter, e.Year;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Sector Growth
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentEconDB.Gold.ZonePerformance AS
SELECT
    ZoneID,
    Industry,
    AVG(TotalJobs) AS AvgJobs,
    COUNT(*) AS ReportingQuarters
FROM GovernmentEconDB.Silver.JobStats
GROUP BY ZoneID, Industry;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Industry has the most jobs in Zone_A?"
    2. "Show avg jobs per industry."
    3. "List all Tech companies registered in 2024."
*/
