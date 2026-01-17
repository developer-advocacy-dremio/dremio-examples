/*
 * Venture Capital Deal Sourcing Demo
 * 
 * Scenario:
 * A VC firm tracks hundreds of startups (Leads) and moves them through a pipeline:
 * Contacted -> Meeting -> Due Diligence -> Term Sheet -> Portfolio.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Analyze deal flow volume and conversion rates by industry.
 * 
 * Note: Assumes a catalog named 'VCPipelineDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS VCPipelineDB;
CREATE FOLDER IF NOT EXISTS VCPipelineDB.Bronze;
CREATE FOLDER IF NOT EXISTS VCPipelineDB.Silver;
CREATE FOLDER IF NOT EXISTS VCPipelineDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Lead Gen
-------------------------------------------------------------------------------
-- Description: Inbound deal flow from scouts, conferences, etc.

CREATE TABLE IF NOT EXISTS VCPipelineDB.Bronze.StartupLeads (
    LeadID VARCHAR,
    StartupName VARCHAR,
    Industry VARCHAR, -- 'SaaS', 'BioTech', 'FinTech', 'AI'
    Source VARCHAR,
    EntryDate DATE,
    CurrentStage VARCHAR
);

-- 1.1 Populate StartupLeads (50+ Records)
INSERT INTO VCPipelineDB.Bronze.StartupLeads (LeadID, StartupName, Industry, Source, EntryDate, CurrentStage) VALUES
('L-001', 'AlphaAI', 'AI', 'Conference', '2025-01-01', 'Portfolio'),
('L-002', 'BetaHealth', 'BioTech', 'Scout', '2025-01-02', 'TermSheet'),
('L-003', 'GammaFin', 'FinTech', 'Referral', '2025-01-03', 'Meeting'),
('L-004', 'DeltaSoft', 'SaaS', 'Scout', '2025-01-04', 'Contacted'),
('L-005', 'EpsilonRobotics', 'AI', 'Conference', '2025-01-05', 'DueDiligence'),
('L-006', 'ZetaBio', 'BioTech', 'Referral', '2025-01-06', 'Rejected'),
('L-007', 'EtaPay', 'FinTech', 'Scout', '2025-01-07', 'Meeting'),
('L-008', 'ThetaCloud', 'SaaS', 'Conference', '2025-01-08', 'Contacted'),
('L-009', 'IotaData', 'AI', 'Referral', '2025-01-09', 'Rejected'),
('L-010', 'KappaMed', 'BioTech', 'Scout', '2025-01-10', 'DueDiligence'),
('L-011', 'LambdaLedger', 'FinTech', 'Conference', '2025-01-11', 'TermSheet'),
('L-012', 'MuSocial', 'SaaS', 'Referral', '2025-01-12', 'Portfolio'),
('L-013', 'NuVision', 'AI', 'Scout', '2025-01-13', 'Meeting'),
('L-014', 'XiPharma', 'BioTech', 'Conference', '2025-01-14', 'Contacted'),
('L-015', 'OmicronChain', 'FinTech', 'Referral', '2025-01-15', 'Rejected'),
('L-016', 'PiAnalytics', 'SaaS', 'Scout', '2025-01-16', 'DueDiligence'),
('L-017', 'RhoRobotics', 'AI', 'Conference', '2025-01-17', 'Rejected'),
('L-018', 'SigmaGen', 'BioTech', 'Referral', '2025-01-18', 'Meeting'),
('L-019', 'TauTrade', 'FinTech', 'Scout', '2025-01-19', 'Contacted'),
('L-020', 'UpsilonAPI', 'SaaS', 'Conference', '2025-01-20', 'Portfolio'),
('L-021', 'PhiNeural', 'AI', 'Referral', '2025-01-21', 'TermSheet'),
('L-022', 'ChiCells', 'BioTech', 'Scout', '2025-01-22', 'Meeting'),
('L-023', 'PsiPayments', 'FinTech', 'Conference', '2025-01-23', 'DueDiligence'),
('L-024', 'OmegaOps', 'SaaS', 'Referral', '2025-01-24', 'Rejected'),
('L-025', 'StartupA', 'AI', 'Scout', '2025-01-25', 'Contacted'),
('L-026', 'StartupB', 'BioTech', 'Conference', '2025-01-26', 'Meeting'),
('L-027', 'StartupC', 'FinTech', 'Referral', '2025-01-27', 'DueDiligence'),
('L-028', 'StartupD', 'SaaS', 'Scout', '2025-01-28', 'Rejected'),
('L-029', 'StartupE', 'AI', 'Conference', '2025-01-29', 'Contacted'),
('L-030', 'StartupF', 'BioTech', 'Referral', '2025-01-30', 'Meeting'),
('L-031', 'StartupG', 'FinTech', 'Scout', '2025-01-31', 'TermSheet'),
('L-032', 'StartupH', 'SaaS', 'Conference', '2025-02-01', 'Portfolio'),
('L-033', 'StartupI', 'AI', 'Referral', '2025-02-02', 'Rejected'),
('L-034', 'StartupJ', 'BioTech', 'Scout', '2025-02-03', 'Contacted'),
('L-035', 'StartupK', 'FinTech', 'Conference', '2025-02-04', 'Meeting'),
('L-036', 'StartupL', 'SaaS', 'Referral', '2025-02-05', 'DueDiligence'),
('L-037', 'StartupM', 'AI', 'Scout', '2025-02-06', 'Rejected'),
('L-038', 'StartupN', 'BioTech', 'Conference', '2025-02-07', 'Contacted'),
('L-039', 'StartupO', 'FinTech', 'Referral', '2025-02-08', 'Meeting'),
('L-040', 'StartupP', 'SaaS', 'Scout', '2025-02-09', 'TermSheet'),
('L-041', 'StartupQ', 'AI', 'Conference', '2025-02-10', 'Portfolio'),
('L-042', 'StartupR', 'BioTech', 'Referral', '2025-02-11', 'Rejected'),
('L-043', 'StartupS', 'FinTech', 'Scout', '2025-02-12', 'Contacted'),
('L-044', 'StartupT', 'SaaS', 'Conference', '2025-02-13', 'Meeting'),
('L-045', 'StartupU', 'AI', 'Referral', '2025-02-14', 'DueDiligence'),
('L-046', 'StartupV', 'BioTech', 'Scout', '2025-02-15', 'Rejected'),
('L-047', 'StartupW', 'FinTech', 'Conference', '2025-02-16', 'Contacted'),
('L-048', 'StartupX', 'SaaS', 'Referral', '2025-02-17', 'Meeting'),
('L-049', 'StartupY', 'AI', 'Scout', '2025-02-18', 'TermSheet'),
('L-050', 'StartupZ', 'BioTech', 'Conference', '2025-02-19', 'Portfolio');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Funnel Management
-------------------------------------------------------------------------------
-- Description: Standardizing stage names and filtering out bad data.

CREATE OR REPLACE VIEW VCPipelineDB.Silver.DealFunnel AS
SELECT
    LeadID,
    StartupName,
    Industry,
    Source,
    CurrentStage,
    CASE 
        WHEN CurrentStage IN ('TermSheet', 'Portfolio') THEN 'Advanced'
        WHEN CurrentStage IN ('Rejected') THEN 'Dead'
        ELSE 'Active'
    END AS StatusCategory
FROM VCPipelineDB.Bronze.StartupLeads;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Strategy Analysis
-------------------------------------------------------------------------------
-- Description: Aggregating by Sector to see where the focus is.
-- Insight: Heatmap of deal volume.

CREATE OR REPLACE VIEW VCPipelineDB.Gold.SectorHeatmap AS
SELECT
    Industry,
    COUNT(*) AS TotalLeads,
    SUM(CASE WHEN StatusCategory = 'Advanced' THEN 1 ELSE 0 END) AS HighQualityDeals,
    SUM(CASE WHEN StatusCategory = 'Advanced' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS ConversionRate
FROM VCPipelineDB.Silver.DealFunnel
GROUP BY Industry;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Industry has the highest ConversionRate in VCPipelineDB.Gold.SectorHeatmap?"

PROMPT:
"Visualize the TotalLeads by Industry using a bar chart."
*/
