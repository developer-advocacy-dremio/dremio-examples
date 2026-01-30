/*
 * FinTech: Crowdfunding Campaign Analytics
 * 
 * Scenario:
 * Analyzing daily pledge velocity and donor sentiment to predict campaign success probability.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS CrowdFundDB;
CREATE FOLDER IF NOT EXISTS CrowdFundDB.Platform;
CREATE FOLDER IF NOT EXISTS CrowdFundDB.Platform.Bronze;
CREATE FOLDER IF NOT EXISTS CrowdFundDB.Platform.Silver;
CREATE FOLDER IF NOT EXISTS CrowdFundDB.Platform.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Campaign Data
-------------------------------------------------------------------------------

-- Campaigns Table
CREATE TABLE IF NOT EXISTS CrowdFundDB.Platform.Bronze.Campaigns (
    CampaignID VARCHAR,
    CreatorID VARCHAR,
    Category VARCHAR, -- Tech, Art, Games, Food
    GoalAmount DOUBLE,
    StartDate DATE,
    EndDate DATE,
    Status VARCHAR -- Active, Suspended, Completed
);

INSERT INTO CrowdFundDB.Platform.Bronze.Campaigns VALUES
('C-001', 'U-100', 'Tech', 50000.0, '2025-01-01', '2025-01-31', 'Active'),
('C-002', 'U-101', 'Games', 20000.0, '2025-01-05', '2025-02-05', 'Active'),
('C-003', 'U-102', 'Art', 5000.0, '2025-01-10', '2025-02-10', 'Active'),
('C-004', 'U-103', 'Food', 15000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-005', 'U-104', 'Tech', 100000.0, '2025-01-15', '2025-02-15', 'Active'),
('C-006', 'U-105', 'Tech', 10000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-007', 'U-106', 'Games', 50000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-008', 'U-107', 'Art', 2000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-009', 'U-108', 'Food', 25000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-010', 'U-109', 'Tech', 75000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-011', 'U-110', 'Games', 30000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-012', 'U-111', 'Art', 8000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-013', 'U-112', 'Food', 10000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-014', 'U-113', 'Tech', 150000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-015', 'U-114', 'Games', 100000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-016', 'U-115', 'Art', 1000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-017', 'U-116', 'Food', 5000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-018', 'U-117', 'Tech', 20000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-019', 'U-118', 'Games', 60000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-020', 'U-119', 'Art', 3000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-021', 'U-120', 'Food', 12000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-022', 'U-121', 'Tech', 200000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-023', 'U-122', 'Games', 15000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-024', 'U-123', 'Art', 15000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-025', 'U-124', 'Food', 40000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-026', 'U-125', 'Tech', 35000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-027', 'U-126', 'Games', 25000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-028', 'U-127', 'Art', 1200.0, '2025-01-01', '2025-02-01', 'Active'),
('C-029', 'U-128', 'Food', 8000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-030', 'U-129', 'Tech', 90000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-031', 'U-130', 'Games', 8000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-032', 'U-131', 'Art', 10000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-033', 'U-132', 'Food', 50000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-034', 'U-133', 'Tech', 120000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-035', 'U-134', 'Games', 40000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-036', 'U-135', 'Art', 7000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-037', 'U-136', 'Food', 3000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-038', 'U-137', 'Tech', 25000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-039', 'U-138', 'Games', 12000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-040', 'U-139', 'Art', 4000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-041', 'U-140', 'Food', 15000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-042', 'U-141', 'Tech', 60000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-043', 'U-142', 'Games', 22000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-044', 'U-143', 'Art', 900.0, '2025-01-01', '2025-02-01', 'Active'),
('C-045', 'U-144', 'Food', 6000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-046', 'U-145', 'Tech', 300000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-047', 'U-146', 'Games', 9000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-048', 'U-147', 'Art', 5500.0, '2025-01-01', '2025-02-01', 'Active'),
('C-049', 'U-148', 'Food', 22000.0, '2025-01-01', '2025-02-01', 'Active'),
('C-050', 'U-149', 'Tech', 45000.0, '2025-01-01', '2025-02-01', 'Active');

-- Pledges Table
CREATE TABLE IF NOT EXISTS CrowdFundDB.Platform.Bronze.Pledges (
    PledgeID VARCHAR,
    CampaignID VARCHAR,
    DonorID VARCHAR,
    Amount DOUBLE,
    PledgeDate DATE
);

INSERT INTO CrowdFundDB.Platform.Bronze.Pledges VALUES
('P-001', 'C-001', 'D-500', 100.0, '2025-01-02'),
('P-002', 'C-001', 'D-501', 50.0, '2025-01-02'),
('P-003', 'C-001', 'D-502', 200.0, '2025-01-03'),
('P-004', 'C-002', 'D-503', 25.0, '2025-01-06'),
('P-005', 'C-002', 'D-504', 25.0, '2025-01-06'),
('P-006', 'C-003', 'D-505', 500.0, '2025-01-11'), -- Early big backer
('P-007', 'C-004', 'D-506', 10.0, '2025-01-02'),
('P-008', 'C-005', 'D-507', 1000.0, '2025-01-16'),
('P-009', 'C-001', 'D-508', 300.0, '2025-01-04'),
('P-010', 'C-002', 'D-509', 50.0, '2025-01-07'),
('P-011', 'C-001', 'D-510', 50.0, '2025-01-02'),
('P-012', 'C-001', 'D-511', 150.0, '2025-01-02'),
('P-013', 'C-006', 'D-512', 20.0, '2025-01-02'),
('P-014', 'C-007', 'D-513', 30.0, '2025-01-02'),
('P-015', 'C-008', 'D-514', 10.0, '2025-01-02'),
('P-016', 'C-009', 'D-515', 50.0, '2025-01-02'),
('P-017', 'C-010', 'D-516', 100.0, '2025-01-02'),
('P-018', 'C-011', 'D-517', 25.0, '2025-01-02'),
('P-019', 'C-012', 'D-518', 40.0, '2025-01-02'),
('P-020', 'C-013', 'D-519', 15.0, '2025-01-02'),
('P-021', 'C-014', 'D-520', 200.0, '2025-01-02'),
('P-022', 'C-015', 'D-521', 120.0, '2025-01-02'),
('P-023', 'C-016', 'D-522', 5.0, '2025-01-02'),
('P-024', 'C-017', 'D-523', 50.0, '2025-01-02'),
('P-025', 'C-018', 'D-524', 25.0, '2025-01-02'),
('P-026', 'C-019', 'D-525', 60.0, '2025-01-02'),
('P-027', 'C-020', 'D-526', 10.0, '2025-01-02'),
('P-028', 'C-021', 'D-527', 30.0, '2025-01-02'),
('P-029', 'C-022', 'D-528', 500.0, '2025-01-02'),
('P-030', 'C-023', 'D-529', 20.0, '2025-01-02'),
('P-031', 'C-024', 'D-530', 20.0, '2025-01-02'),
('P-032', 'C-025', 'D-531', 40.0, '2025-01-02'),
('P-033', 'C-026', 'D-532', 35.0, '2025-01-02'),
('P-034', 'C-027', 'D-533', 25.0, '2025-01-02'),
('P-035', 'C-028', 'D-534', 5.0, '2025-01-02'),
('P-036', 'C-029', 'D-535', 80.0, '2025-01-02'),
('P-037', 'C-030', 'D-536', 90.0, '2025-01-02'),
('P-038', 'C-031', 'D-537', 10.0, '2025-01-02'),
('P-039', 'C-032', 'D-538', 10.0, '2025-01-02'),
('P-040', 'C-033', 'D-539', 50.0, '2025-01-02'),
('P-041', 'C-034', 'D-540', 120.0, '2025-01-02'),
('P-042', 'C-035', 'D-541', 40.0, '2025-01-02'),
('P-043', 'C-036', 'D-542', 7.0, '2025-01-02'),
('P-044', 'C-037', 'D-543', 30.0, '2025-01-02'),
('P-045', 'C-038', 'D-544', 25.0, '2025-01-02'),
('P-046', 'C-039', 'D-545', 12.0, '2025-01-02'),
('P-047', 'C-040', 'D-546', 4.0, '2025-01-02'),
('P-048', 'C-041', 'D-547', 15.0, '2025-01-02'),
('P-049', 'C-042', 'D-548', 60.0, '2025-01-02'),
('P-050', 'C-043', 'D-549', 22.0, '2025-01-02');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Campaign Performance
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CrowdFundDB.Platform.Silver.CampaignProgress AS
SELECT 
    c.CampaignID,
    c.Category,
    c.GoalAmount,
    c.StartDate,
    SUM(p.Amount) AS TotalPledged,
    COUNT(p.DonorID) AS BackerCount,
    -- Calculate % Funded
    (SUM(p.Amount) / c.GoalAmount) * 100.0 AS PercentFunded,
    -- Calculate Daily Run Rate (Linear Projection)
    SUM(p.Amount) / CASE 
        WHEN DATE_DIFF(CURRENT_DATE, c.StartDate) = 0 THEN 1 
        ELSE DATE_DIFF(CURRENT_DATE, c.StartDate) 
    END AS DailyPledgeRate
FROM CrowdFundDB.Platform.Bronze.Campaigns c
LEFT JOIN CrowdFundDB.Platform.Bronze.Pledges p ON c.CampaignID = p.CampaignID
WHERE c.Status = 'Active'
GROUP BY c.CampaignID, c.Category, c.GoalAmount, c.StartDate;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Success Prediction
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CrowdFundDB.Platform.Gold.SuccessProjection AS
SELECT 
    CampaignID,
    Category,
    GoalAmount,
    TotalPledged,
    PercentFunded,
    DailyPledgeRate,
    -- Projection: Current + (DailyRate * RemainingDays)
    TotalPledged + (DailyPledgeRate * 30) AS ProjectedTotal, -- Assuming ~30 days left for simplicity
    CASE 
        WHEN (TotalPledged + (DailyPledgeRate * 30)) >= GoalAmount THEN 'Likely Success'
        WHEN PercentFunded < 10 AND DailyPledgeRate < 50 THEN 'High Risk of Failure'
        ELSE 'Uncertain'
    END AS Prediction
FROM CrowdFundDB.Platform.Silver.CampaignProgress;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify all campaigns in the 'Tech' category that are predicted to be a 'Likely Success'."

PROMPT 2:
"Calculate the average backer adoption rate (BackerCount per day) for campaigns in the Silver layer."

PROMPT 3:
"List the top 3 campaigns with the highest 'PercentFunded' currently."
*/
