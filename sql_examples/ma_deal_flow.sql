/*
 * M&A Deal Flow Demo
 * 
 * Scenario:
 * Investment bankers tracking activity in a Virtual Data Room (VDR) to gauge buyer interest.
 * 
 * Data Context:
 * - Deals: Active M&A projects (Project X, Project Y).
 * - VDR_Activity: Logs of potential buyers viewing documents.
 * 
 * Analytical Goal:
 * Predict which buyers are serious based on document dwel time and download counts.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS InvestmentBankDB;
CREATE FOLDER IF NOT EXISTS InvestmentBankDB.Bronze;
CREATE FOLDER IF NOT EXISTS InvestmentBankDB.Silver;
CREATE FOLDER IF NOT EXISTS InvestmentBankDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS InvestmentBankDB.Bronze.Deals (
    DealID VARCHAR,
    TargetCompany VARCHAR,
    Sector VARCHAR,
    DealSizeMillions DOUBLE
);

CREATE TABLE IF NOT EXISTS InvestmentBankDB.Bronze.VDR_Activity (
    LogID INT,
    DealID VARCHAR,
    BuyerFirm VARCHAR, -- 'PE_Firm_A', 'Corp_Buyer_B'
    DocName VARCHAR, -- 'Financials.xls', 'Legal.pdf'
    Action VARCHAR, -- 'View', 'Download'
    DurationSeconds INT,
    "Date" DATE
);

INSERT INTO InvestmentBankDB.Bronze.Deals VALUES
('D-001', 'TechStart Inc', 'Technology', 500.0),
('D-002', 'BioHealth Corp', 'Healthcare', 200.0),
('D-003', 'GreenEnergy Co', 'Energy', 1200.0);

INSERT INTO InvestmentBankDB.Bronze.VDR_Activity VALUES
(1, 'D-001', 'PE_Firm_A', 'Financials.xls', 'Download', 0, '2025-12-01'),
(2, 'D-001', 'PE_Firm_A', 'Legal.pdf', 'View', 300, '2025-12-01'),
(3, 'D-001', 'Corp_Buyer_B', 'Financials.xls', 'View', 60, '2025-12-02'), -- Low interest?
(4, 'D-001', 'PE_Firm_C', 'Financials.xls', 'Download', 0, '2025-12-02'),
(5, 'D-001', 'PE_Firm_C', 'Legal.pdf', 'View', 1200, '2025-12-03'), -- Deep dive
(6, 'D-002', 'Pharma_Giant', 'IP_Patents.pdf', 'Download', 0, '2025-12-01'),
(7, 'D-002', 'Pharma_Giant', 'ClinicalTrials.xls', 'View', 500, '2025-12-01'),
(8, 'D-003', 'Oil_Major', 'Geology_Report.pdf', 'View', 10, '2025-12-01'),
(9, 'D-001', 'PE_Firm_A', 'HR_OrgChart.pdf', 'View', 200, '2025-12-04'),
(10, 'D-001', 'PE_Firm_C', 'CapTable.xls', 'Download', 0, '2025-12-04');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW InvestmentBankDB.Silver.BuyerInterest AS
SELECT 
    DealID,
    BuyerFirm,
    COUNT(*) AS TotalInteractions,
    SUM(CASE WHEN Action = 'Download' THEN 1 ELSE 0 END) AS Downloads,
    SUM(DurationSeconds) AS TotalViewTimeSeconds
FROM InvestmentBankDB.Bronze.VDR_Activity
GROUP BY DealID, BuyerFirm;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW InvestmentBankDB.Gold.DealHeatmap AS
SELECT 
    DealID,
    BuyerFirm,
    Downloads,
    TotalViewTimeSeconds,
    CASE 
        WHEN Downloads > 2 AND TotalViewTimeSeconds > 600 THEN 'High Interest'
        WHEN Downloads > 0 THEN 'Moderate'
        ELSE 'Tire Kicker'
    END AS BuyerStatus
FROM InvestmentBankDB.Silver.BuyerInterest;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all buyers with 'High Interest' for DealID 'D-001' in InvestmentBankDB.Gold.DealHeatmap."
*/
