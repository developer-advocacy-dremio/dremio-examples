/*
 * Insurance Risk Analysis Demo
 * 
 * Scenario:
 * An insurance provider analyzes loss ratios to adjust premiums and identify high-risk regions.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Maximize profitability and monitor claims.
 * 
 * Note: Assumes a catalog named 'InsuranceDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS InsuranceDB;
CREATE FOLDER IF NOT EXISTS InsuranceDB.Bronze;
CREATE FOLDER IF NOT EXISTS InsuranceDB.Silver;
CREATE FOLDER IF NOT EXISTS InsuranceDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS InsuranceDB.Bronze.PolicyHolders (
    PolicyHolderID INT,
    Name VARCHAR,
    Age INT,
    Region VARCHAR,
    CreditScore INT
);

CREATE TABLE IF NOT EXISTS InsuranceDB.Bronze.Policies (
    PolicyID INT,
    PolicyHolderID INT,
    PolicyType VARCHAR, -- 'Auto', 'Home', 'Life', etc.
    StartDate DATE,
    EndDate DATE,
    Premium DOUBLE
);

CREATE TABLE IF NOT EXISTS InsuranceDB.Bronze.Claims (
    ClaimID INT,
    PolicyID INT,
    ClaimDate DATE,
    ClaimAmount DOUBLE,
    Status VARCHAR
);

-- 1.2 Populate Bronze Tables
-- Insert 20 PolicyHolders
INSERT INTO InsuranceDB.Bronze.PolicyHolders (PolicyHolderID, Name, Age, Region, CreditScore) VALUES
(1, 'Alice Johnson', 34, 'North', 720),
(2, 'Bob Smith', 45, 'South', 680),
(3, 'Charlie Brown', 29, 'East', 750),
(4, 'Diana Prince', 52, 'West', 800),
(5, 'Evan Wright', 41, 'North', 650),
(6, 'Fiona Hill', 38, 'South', 710),
(7, 'George King', 60, 'East', 790),
(8, 'Hannah Scott', 25, 'West', 690),
(9, 'Ian Green', 47, 'North', 730),
(10, 'Julia Baker', 33, 'South', 760),
(11, 'Kevin Adams', 55, 'East', 640),
(12, 'Laura Carter', 31, 'West', 780),
(13, 'Mike Nelson', 40, 'North', 700),
(14, 'Nina Turner', 28, 'South', 740),
(15, 'Oscar Rivera', 49, 'East', 660),
(16, 'Paula Mitchell', 36, 'West', 810),
(17, 'Quinn Campbell', 44, 'North', 670),
(18, 'Rachel Phillips', 30, 'South', 725),
(19, 'Steve Parker', 58, 'East', 770),
(20, 'Tina Evans', 27, 'West', 695);

-- Insert 30 Policies
INSERT INTO InsuranceDB.Bronze.Policies (PolicyID, PolicyHolderID, PolicyType, StartDate, EndDate, Premium) VALUES
(101, 1, 'Auto', '2024-01-01', '2025-01-01', 1200.00),
(102, 1, 'Home', '2024-02-01', '2025-02-01', 950.00),
(103, 2, 'Life', '2023-06-15', '2043-06-15', 500.00),
(104, 3, 'Auto', '2024-03-10', '2025-03-10', 1100.00),
(105, 4, 'Home', '2023-11-20', '2024-11-20', 1500.00),
(106, 5, 'Auto', '2024-01-15', '2025-01-15', 1300.00),
(107, 6, 'Life', '2022-08-01', '2042-08-01', 600.00),
(108, 7, 'Home', '2024-05-05', '2025-05-05', 1800.00),
(109, 8, 'Auto', '2024-04-01', '2025-04-01', 1050.00),
(110, 9, 'Life', '2023-12-01', '2033-12-01', 450.00),
(111, 10, 'Auto', '2024-02-28', '2025-02-28', 1250.00),
(112, 11, 'Home', '2023-10-10', '2024-10-10', 1100.00),
(113, 12, 'Auto', '2024-06-01', '2025-06-01', 1150.00),
(114, 13, 'Life', '2024-01-01', '2044-01-01', 550.00),
(115, 14, 'Home', '2024-03-15', '2025-03-15', 1400.00),
(116, 15, 'Auto', '2023-09-01', '2024-09-01', 1280.00),
(117, 16, 'Life', '2021-07-20', '2041-07-20', 700.00),
(118, 17, 'Home', '2024-01-20', '2025-01-20', 1000.00),
(119, 18, 'Auto', '2024-05-15', '2025-05-15', 1180.00),
(120, 19, 'Life', '2020-03-01', '2040-03-01', 800.00),
(121, 20, 'Auto', '2024-07-01', '2025-07-01', 1120.00),
(122, 1, 'Life', '2024-01-01', '2034-01-01', 400.00),
(123, 2, 'Auto', '2024-08-01', '2025-08-01', 1220.00),
(124, 3, 'Home', '2024-04-10', '2025-04-10', 1350.00),
(125, 4, 'Auto', '2023-12-15', '2024-12-15', 1400.00),
(126, 5, 'Life', '2024-02-10', '2044-02-10', 480.00),
(127, 6, 'Home', '2024-06-20', '2025-06-20', 1600.00),
(128, 7, 'Auto', '2024-03-05', '2025-03-05', 1100.00),
(129, 8, 'Life', '2023-11-01', '2033-11-01', 520.00),
(130, 9, 'Home', '2024-01-30', '2025-01-30', 1250.00);

-- Insert 15 Claims
INSERT INTO InsuranceDB.Bronze.Claims (ClaimID, PolicyID, ClaimDate, ClaimAmount, Status) VALUES
(1, 101, '2024-03-15', 500.00, 'Paid'),
(2, 104, '2024-04-20', 1200.00, 'Paid'),
(3, 105, '2024-01-10', 3500.00, 'Pending'),
(4, 106, '2024-02-28', 800.00, 'Paid'),
(5, 108, '2024-06-01', 5000.00, 'Review'),
(6, 111, '2024-05-15', 1500.00, 'Paid'),
(7, 113, '2024-07-10', 200.00, 'Paid'),
(8, 115, '2024-04-05', 2500.00, 'Denied'),
(9, 116, '2024-01-25', 4000.00, 'Paid'),
(10, 118, '2024-03-30', 1800.00, 'Pending'),
(11, 121, '2024-07-20', 600.00, 'Paid'),
(12, 123, '2024-08-15', 900.00, 'Review'),
(13, 125, '2024-02-10', 3000.00, 'Paid'),
(14, 127, '2024-06-25', 1500.00, 'Paid'),
(15, 130, '2024-05-01', 2200.00, 'Pending');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Enrichment & Activity
-------------------------------------------------------------------------------

-- 2.1 View: Active_Policies
-- Filters for currently active policies and joins holder details.
CREATE OR REPLACE VIEW InsuranceDB.Silver.Active_Policies AS
SELECT
    p.PolicyID,
    ph.Name AS HolderName,
    ph.Region,
    ph.CreditScore,
    p.PolicyType,
    p.StartDate,
    p.EndDate,
    p.Premium
FROM InsuranceDB.Bronze.Policies p
JOIN InsuranceDB.Bronze.PolicyHolders ph ON p.PolicyHolderID = ph.PolicyHolderID
WHERE p.EndDate > CAST('2025-05-01' AS DATE); -- Assumed current date

-- 2.2 View: Claims_History
-- Joins Claims with Policy and Holder info.
CREATE OR REPLACE VIEW InsuranceDB.Silver.Claims_History AS
SELECT
    c.ClaimID,
    c.ClaimDate,
    c.ClaimAmount,
    c.Status,
    p.PolicyType,
    ph.Region,
    ph.CreditScore
FROM InsuranceDB.Bronze.Claims c
JOIN InsuranceDB.Bronze.Policies p ON c.PolicyID = p.PolicyID
JOIN InsuranceDB.Bronze.PolicyHolders ph ON p.PolicyHolderID = ph.PolicyHolderID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Risk & Profitability
-------------------------------------------------------------------------------

-- 3.1 View: Loss_Ratio_By_Region
-- Calculates Loss Ratio (Total Claims / Total Premiums) per region.
CREATE OR REPLACE VIEW InsuranceDB.Gold.Loss_Ratio_By_Region AS
SELECT
    ap.Region,
    SUM(ap.Premium) AS TotalPremiumsCollected,
    COALESCE(SUM(ch.ClaimAmount), 0) AS TotalClaimsPaid,
    CASE 
        WHEN SUM(ap.Premium) > 0 THEN (COALESCE(SUM(ch.ClaimAmount), 0) / SUM(ap.Premium)) * 100 
        ELSE 0 
    END AS LossRatioPct
FROM InsuranceDB.Silver.Active_Policies ap
LEFT JOIN InsuranceDB.Silver.Claims_History ch ON ap.Region = ch.Region
GROUP BY ap.Region;

-- 3.2 View: Product_Profitability
-- Analyzes profitability by Policy Type.
CREATE OR REPLACE VIEW InsuranceDB.Gold.Product_Profitability AS
SELECT
    PolicyType,
    COUNT(DISTINCT PolicyID) AS PolicyCount,
    SUM(Premium) AS TotalRevenue,
    (SUM(Premium) - (SELECT SUM(ClaimAmount) FROM InsuranceDB.Silver.Claims_History WHERE PolicyType = PolicyType)) AS EstimatedProfit
FROM InsuranceDB.Silver.Active_Policies
GROUP BY PolicyType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Risk Analysis):
"Using InsuranceDB.Gold.Loss_Ratio_By_Region, create a heatmap of LossRatioPct by Region."

PROMPT 2 (Claims Monitor):
"List all claims from InsuranceDB.Silver.Claims_History where Status is 'Pending' and ClaimAmount > 10000."

PROMPT 3 (Demographics):
"From InsuranceDB.Silver.Active_Policies, what is the average CreditScore for each PolicyType?"
*/
