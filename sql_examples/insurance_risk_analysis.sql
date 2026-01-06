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
    Premium FLOAT
);

CREATE TABLE IF NOT EXISTS InsuranceDB.Bronze.Claims (
    ClaimID INT,
    PolicyID INT,
    ClaimDate DATE,
    ClaimAmount FLOAT,
    Status VARCHAR
);

-- 1.2 Populate Bronze Tables

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
