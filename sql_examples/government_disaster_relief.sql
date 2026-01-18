/*
    Dremio High-Volume SQL Pattern: Government Disaster Relief
    
    Business Scenario:
    FEMA/Emergency Agencies must track "Grant Allocations" vs "Disbursements" during disaster recovery.
    Timely payout while preventing fraud (Duplicate Claims) is the goal.
    
    Data Story:
    We track Victims/Claimants and Fund Disbursements.
    
    Medallion Architecture:
    - Bronze: Claimants, FundAllocations.
      *Volume*: 50+ records.
    - Silver: PayoutStatus (Claimed vs Paid).
    - Gold: FundBurnRate (Remaining Budget per Zone).
    
    Key Dremio Features:
    - SUM aggregations
    - Balance Calculations
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentDisasterDB;
CREATE FOLDER IF NOT EXISTS GovernmentDisasterDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentDisasterDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentDisasterDB.Gold;
USE GovernmentDisasterDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentDisasterDB.Bronze.FundAllocations (
    ZoneID STRING,
    ZoneName STRING,
    TotalBudget DOUBLE
);

INSERT INTO GovernmentDisasterDB.Bronze.FundAllocations VALUES
('Z1', 'Coastal Region', 1000000.0), -- 1M
('Z2', 'Inland Valley', 500000.0),   -- 500k
('Z3', 'Mountain District', 250000.0); -- 250k

CREATE OR REPLACE TABLE GovernmentDisasterDB.Bronze.Claimants (
    ClaimID STRING,
    ZoneID STRING,
    HeadOfHousehold STRING,
    DamageAssessment DOUBLE,
    ApprovedAmount DOUBLE,
    PaidAmount DOUBLE,
    Status STRING -- Approved, Pending, Denied
);

-- Bulk Claims (50 Records)
INSERT INTO GovernmentDisasterDB.Bronze.Claimants VALUES
('CLM1001', 'Z1', 'Family 1', 50000.0, 40000.0, 40000.0, 'Approved'),
('CLM1002', 'Z1', 'Family 2', 10000.0, 0.0, 0.0, 'Denied'),
('CLM1003', 'Z1', 'Family 3', 75000.0, 60000.0, 30000.0, 'Approved'), -- Partial Pay
('CLM1004', 'Z1', 'Family 4', 20000.0, 15000.0, 15000.0, 'Approved'),
('CLM1005', 'Z2', 'Family 5', 5000.0, 4000.0, 4000.0, 'Approved'),
('CLM1006', 'Z2', 'Family 6', 12000.0, 10000.0, 0.0, 'Approved'), -- Pending pay
('CLM1007', 'Z2', 'Family 7', 8000.0, 0.0, 0.0, 'Denied'),
('CLM1008', 'Z3', 'Family 8', 30000.0, 25000.0, 25000.0, 'Approved'),
('CLM1009', 'Z3', 'Family 9', 40000.0, 35000.0, 0.0, 'Approved'),
('CLM1010', 'Z1', 'Family 10', 60000.0, 50000.0, 50000.0, 'Approved'),
('CLM1011', 'Z1', 'Family 11', 15000.0, 12000.0, 12000.0, 'Approved'),
('CLM1012', 'Z1', 'Family 12', 5000.0, 0.0, 0.0, 'Denied'),
('CLM1013', 'Z2', 'Family 13', 25000.0, 20000.0, 20000.0, 'Approved'),
('CLM1014', 'Z2', 'Family 14', 35000.0, 30000.0, 15000.0, 'Approved'),
('CLM1015', 'Z3', 'Family 15', 10000.0, 8000.0, 8000.0, 'Approved'),
('CLM1016', 'Z1', 'Family 16', 90000.0, 75000.0, 75000.0, 'Approved'),
('CLM1017', 'Z1', 'Family 17', 2000.0, 0.0, 0.0, 'Denied'),
('CLM1018', 'Z1', 'Family 18', 45000.0, 35000.0, 35000.0, 'Approved'),
('CLM1019', 'Z2', 'Family 19', 18000.0, 15000.0, 0.0, 'Approved'),
('CLM1020', 'Z2', 'Family 20', 6000.0, 5000.0, 5000.0, 'Approved'),
('CLM1021', 'Z3', 'Family 21', 50000.0, 40000.0, 40000.0, 'Approved'),
('CLM1022', 'Z3', 'Family 22', 12000.0, 0.0, 0.0, 'Denied'),
('CLM1023', 'Z1', 'Family 23', 80000.0, 65000.0, 65000.0, 'Approved'),
('CLM1024', 'Z1', 'Family 24', 25000.0, 20000.0, 0.0, 'Approved'),
('CLM1025', 'Z1', 'Family 25', 10000.0, 8000.0, 8000.0, 'Approved'),
('CLM1026', 'Z2', 'Family 26', 30000.0, 25000.0, 25000.0, 'Approved'),
('CLM1027', 'Z2', 'Family 27', 7000.0, 0.0, 0.0, 'Denied'),
('CLM1028', 'Z3', 'Family 28', 15000.0, 12000.0, 12000.0, 'Approved'),
('CLM1029', 'Z1', 'Family 29', 55000.0, 45000.0, 45000.0, 'Approved'),
('CLM1030', 'Z1', 'Family 30', 5000.0, 4000.0, 4000.0, 'Approved'),
('CLM1031', 'Z1', 'Family 31', 100000.0, 80000.0, 0.0, 'Approved'), -- Big pending
('CLM1032', 'Z2', 'Family 32', 40000.0, 32000.0, 32000.0, 'Approved'),
('CLM1033', 'Z2', 'Family 33', 9000.0, 0.0, 0.0, 'Denied'),
('CLM1034', 'Z3', 'Family 34', 20000.0, 16000.0, 16000.0, 'Approved'),
('CLM1035', 'Z3', 'Family 35', 60000.0, 50000.0, 25000.0, 'Approved'),
('CLM1036', 'Z1', 'Family 36', 30000.0, 24000.0, 24000.0, 'Approved'),
('CLM1037', 'Z1', 'Family 37', 15000.0, 12000.0, 0.0, 'Approved'),
('CLM1038', 'Z1', 'Family 38', 5000.0, 0.0, 0.0, 'Denied'),
('CLM1039', 'Z1', 'Family 39', 70000.0, 55000.0, 55000.0, 'Approved'),
('CLM1040', 'Z2', 'Family 40', 22000.0, 18000.0, 18000.0, 'Approved'),
('CLM1041', 'Z2', 'Family 41', 8000.0, 6000.0, 0.0, 'Approved'),
('CLM1042', 'Z3', 'Family 42', 25000.0, 20000.0, 20000.0, 'Approved'),
('CLM1043', 'Z3', 'Family 43', 12000.0, 0.0, 0.0, 'Denied'),
('CLM1044', 'Z1', 'Family 44', 95000.0, 75000.0, 75000.0, 'Approved'),
('CLM1045', 'Z1', 'Family 45', 18000.0, 15000.0, 15000.0, 'Approved'),
('CLM1046', 'Z1', 'Family 46', 4000.0, 0.0, 0.0, 'Denied'),
('CLM1047', 'Z2', 'Family 47', 35000.0, 28000.0, 28000.0, 'Approved'),
('CLM1048', 'Z2', 'Family 48', 10000.0, 8000.0, 0.0, 'Approved'),
('CLM1049', 'Z3', 'Family 49', 50000.0, 40000.0, 40000.0, 'Approved'),
('CLM1050', 'Z3', 'Family 50', 5000.0, 4000.0, 4000.0, 'Approved');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Claim Ledger
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentDisasterDB.Silver.ClaimLedger AS
SELECT
    c.ClaimID,
    c.ZoneID,
    f.ZoneName,
    c.DamageAssessment,
    c.ApprovedAmount,
    c.PaidAmount,
    (c.ApprovedAmount - c.PaidAmount) AS PendingPayout
FROM GovernmentDisasterDB.Bronze.Claimants c
JOIN GovernmentDisasterDB.Bronze.FundAllocations f ON c.ZoneID = f.ZoneID
WHERE c.Status = 'Approved';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Budget Burn Rate
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentDisasterDB.Gold.FundUtilization AS
SELECT
    f.ZoneName,
    f.TotalBudget,
    COALESCE(SUM(l.PaidAmount), 0) AS TotalDisbursed,
    COALESCE(SUM(l.PendingPayout), 0) AS CommittedPending,
    (f.TotalBudget - COALESCE(SUM(l.ApprovedAmount), 0)) AS RemainingBudget,
    (COALESCE(SUM(l.ApprovedAmount), 0) / f.TotalBudget) * 100 AS UtilizationPct
FROM GovernmentDisasterDB.Bronze.FundAllocations f
LEFT JOIN GovernmentDisasterDB.Silver.ClaimLedger l ON f.ZoneID = l.ZoneID
GROUP BY f.ZoneName, f.TotalBudget;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Zone has utilized the highest percentage of its budget?"
    2. "Show Total Disbursed amount by Zone."
    3. "List remaining budget for Coastal Region."
*/
