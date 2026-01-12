/*
 * Investment Banking Deal Flow Demo
 * 
 * Scenario:
 * Tracking M&A deal pipelines and potential fee revenue by industry sector.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.IB;
CREATE FOLDER IF NOT EXISTS RetailDB.IB.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.IB.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.IB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.IB.Bronze.Deals (
    DealID INT,
    ClientName VARCHAR,
    TargetCompany VARCHAR,
    Sector VARCHAR,
    EstimatedDealValue DOUBLE,
    Stage VARCHAR, -- Pitch, Due Diligence, Closed
    FeePercent DOUBLE
);

INSERT INTO RetailDB.IB.Bronze.Deals VALUES
(1, 'TechCorp', 'SmallStartup', 'Technology', 50000000, 'Closed', 0.02),
(2, 'BigBank', 'RegionalBank', 'Financials', 1200000000, 'Due Diligence', 0.015),
(3, 'AutoGiant', 'EV_Maker', 'Industrials', 300000000, 'Pitch', 0.02),
(4, 'HealthPlus', 'BioFirm', 'Healthcare', 75000000, 'Closed', 0.025),
(5, 'MegaRetail', 'EcomSite', 'Consumer', 150000000, 'Due Diligence', 0.018),
(6, 'EnergyStar', 'SolarField', 'Energy', 200000000, 'Pitch', 0.02),
(7, 'FoodCo', 'SnackBrand', 'Consumer', 50000000, 'Closed', 0.02),
(8, 'MediaGrp', 'StreamService', 'Communication', 600000000, 'Pitch', 0.015),
(9, 'SoftSystems', 'AI_Lab', 'Technology', 90000000, 'Due Diligence', 0.03),
(10, 'RealtyTrust', 'OfficePark', 'Real Estate', 110000000, 'Closed', 0.01),
(11, 'ChemInd', 'PlasticsCo', 'Materials', 45000000, 'Pitch', 0.022);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.IB.Silver.ProjectedFees AS
SELECT 
    DealID,
    Sector,
    Stage,
    EstimatedDealValue,
    (EstimatedDealValue * FeePercent) AS ProjectedFee,
    CASE 
        WHEN Stage = 'Closed' THEN 1.0
        WHEN Stage = 'Due Diligence' THEN 0.5
        ELSE 0.1
    END AS Probability
FROM RetailDB.IB.Bronze.Deals;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.IB.Gold.RevenuePipeline AS
SELECT 
    Sector,
    SUM(ProjectedFee) AS TotalPotentialFees,
    SUM(ProjectedFee * Probability) AS RiskAdjustedFees
FROM RetailDB.IB.Silver.ProjectedFees
GROUP BY Sector;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show me the bar chart of RiskAdjustedFees by Sector from RetailDB.IB.Gold.RevenuePipeline."
*/
