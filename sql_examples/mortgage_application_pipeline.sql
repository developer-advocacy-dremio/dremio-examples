/*
 * Mortgage Application Pipeline Demo
 * 
 * Scenario:
 * A retail bank wants to track the efficiency of its mortgage origination process.
 * Key metrics: Time to Decision, Approval Rates, and Funnel drop-off.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize the underwriting workflow.
 * 
 * Note: Assumes a catalog named 'LendingDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS LendingDB;
CREATE FOLDER IF NOT EXISTS LendingDB.Bronze;
CREATE FOLDER IF NOT EXISTS LendingDB.Silver;
CREATE FOLDER IF NOT EXISTS LendingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Application Data
-------------------------------------------------------------------------------
-- Description: Raw application forms and system logs from the underwriting engine.

CREATE TABLE IF NOT EXISTS LendingDB.Bronze.Applications (
    AppID VARCHAR,
    ApplicantID VARCHAR,
    LoanAmount DOUBLE,
    PropertyState VARCHAR,
    AppDate DATE,
    ProductType VARCHAR -- 'Fixed30', 'ARM5', 'Fixed15'
);

CREATE TABLE IF NOT EXISTS LendingDB.Bronze.UnderwriterLog (
    LogID VARCHAR,
    AppID VARCHAR,
    Stage VARCHAR, -- 'Received', 'DocReview', 'Appraisal', 'FinalDecision'
    Status VARCHAR, -- 'Pass', 'Fail', 'Approved', 'Rejected'
    LogTime TIMESTAMP,
    UnderwriterID VARCHAR
);

-- 1.1 Populate Applications (50+ Records)
INSERT INTO LendingDB.Bronze.Applications (AppID, ApplicantID, LoanAmount, PropertyState, AppDate, ProductType) VALUES
('APP-001', 'C-1001', 350000, 'NY', '2025-01-01', 'Fixed30'),
('APP-002', 'C-1002', 450000, 'CA', '2025-01-01', 'ARM5'),
('APP-003', 'C-1003', 250000, 'TX', '2025-01-02', 'Fixed15'),
('APP-004', 'C-1004', 550000, 'FL', '2025-01-02', 'Fixed30'),
('APP-005', 'C-1005', 300000, 'IL', '2025-01-03', 'Fixed30'),
('APP-006', 'C-1006', 600000, 'NY', '2025-01-03', 'ARM5'),
('APP-007', 'C-1007', 200000, 'OH', '2025-01-04', 'Fixed15'),
('APP-008', 'C-1008', 400000, 'WA', '2025-01-04', 'Fixed30'),
('APP-009', 'C-1009', 320000, 'TX', '2025-01-05', 'Fixed30'),
('APP-010', 'C-1010', 500000, 'CA', '2025-01-05', 'Fixed30'),
('APP-011', 'C-1011', 280000, 'AZ', '2025-01-06', 'Fixed15'),
('APP-012', 'C-1012', 370000, 'NC', '2025-01-06', 'ARM5'),
('APP-013', 'C-1013', 420000, 'GA', '2025-01-07', 'Fixed30'),
('APP-014', 'C-1014', 310000, 'MI', '2025-01-07', 'Fixed30'),
('APP-015', 'C-1015', 520000, 'CO', '2025-01-08', 'ARM5'),
('APP-016', 'C-1016', 290000, 'FL', '2025-01-08', 'Fixed30'),
('APP-017', 'C-1017', 480000, 'NY', '2025-01-09', 'Fixed30'),
('APP-018', 'C-1018', 340000, 'TX', '2025-01-09', 'Fixed15'),
('APP-019', 'C-1019', 410000, 'PA', '2025-01-10', 'Fixed30'),
('APP-020', 'C-1020', 360000, 'VA', '2025-01-10', 'Fixed30'),
('APP-021', 'C-1021', 430000, 'MA', '2025-01-11', 'ARM5'),
('APP-022', 'C-1022', 270000, 'TN', '2025-01-11', 'Fixed15'),
('APP-023', 'C-1023', 580000, 'CA', '2025-01-12', 'Fixed30'),
('APP-024', 'C-1024', 330000, 'NV', '2025-01-12', 'Fixed30'),
('APP-025', 'C-1025', 460000, 'NJ', '2025-01-13', 'Fixed30'),
('APP-026', 'C-1026', 390000, 'OR', '2025-01-13', 'ARM5'),
('APP-027', 'C-1027', 240000, 'IN', '2025-01-14', 'Fixed15'),
('APP-028', 'C-1028', 510000, 'WA', '2025-01-14', 'Fixed30'),
('APP-029', 'C-1029', 305000, 'SC', '2025-01-15', 'Fixed30'),
('APP-030', 'C-1030', 490000, 'MD', '2025-01-15', 'Fixed30'),
('APP-031', 'C-1031', 260000, 'KY', '2025-01-16', 'Fixed15'),
('APP-032', 'C-1032', 440000, 'MN', '2025-01-16', 'ARM5'),
('APP-033', 'C-1033', 375000, 'WI', '2025-01-17', 'Fixed30'),
('APP-034', 'C-1034', 425000, 'MO', '2025-01-17', 'Fixed30'),
('APP-035', 'C-1035', 315000, 'AL', '2025-01-18', 'Fixed30'),
('APP-036', 'C-1036', 530000, 'CA', '2025-01-18', 'ARM5'),
('APP-037', 'C-1037', 230000, 'OK', '2025-01-19', 'Fixed15'),
('APP-038', 'C-1038', 470000, 'CT', '2025-01-19', 'Fixed30'),
('APP-039', 'C-1039', 355000, 'UT', '2025-01-20', 'Fixed30'),
('APP-040', 'C-1040', 295000, 'IA', '2025-01-20', 'Fixed30'),
('APP-041', 'C-1041', 505000, 'NY', '2025-01-21', 'ARM5'),
('APP-042', 'C-1042', 380000, 'TX', '2025-01-21', 'Fixed30'),
('APP-043', 'C-1043', 255000, 'KS', '2025-01-22', 'Fixed15'),
('APP-044', 'C-1044', 495000, 'FL', '2025-01-22', 'Fixed30'),
('APP-045', 'C-1045', 325000, 'LA', '2025-01-23', 'Fixed30'),
('APP-046', 'C-1046', 415000, 'IL', '2025-01-23', 'ARM5'),
('APP-047', 'C-1047', 285000, 'AR', '2025-01-24', 'Fixed15'),
('APP-048', 'C-1048', 540000, 'CA', '2025-01-24', 'Fixed30'),
('APP-049', 'C-1049', 365000, 'MS', '2025-01-25', 'Fixed30'),
('APP-050', 'C-1050', 435000, 'GA', '2025-01-25', 'Fixed30');

-- 1.2 Populate UnderwriterLog (Samples)
INSERT INTO LendingDB.Bronze.UnderwriterLog (LogID, AppID, Stage, Status, LogTime, UnderwriterID) VALUES
('L-001', 'APP-001', 'Received', 'Pass', TIMESTAMP '2025-01-01 10:00:00', 'SYS'),
('L-002', 'APP-001', 'DocReview', 'Pass', TIMESTAMP '2025-01-02 11:00:00', 'U-01'),
('L-003', 'APP-001', 'Appraisal', 'Pass', TIMESTAMP '2025-01-05 14:00:00', 'U-05'),
('L-004', 'APP-001', 'FinalDecision', 'Approved', TIMESTAMP '2025-01-06 09:00:00', 'U-01'),
('L-005', 'APP-002', 'Received', 'Pass', TIMESTAMP '2025-01-01 12:00:00', 'SYS'),
('L-006', 'APP-002', 'DocReview', 'Fail', TIMESTAMP '2025-01-03 10:00:00', 'U-02'), -- Missing docs
('L-007', 'APP-003', 'Received', 'Pass', TIMESTAMP '2025-01-02 09:00:00', 'SYS'),
('L-008', 'APP-003', 'FinalDecision', 'Approved', TIMESTAMP '2025-01-02 10:00:00', 'AUTO'), -- Auto approval
('L-009', 'APP-004', 'Received', 'Pass', TIMESTAMP '2025-01-02 15:00:00', 'SYS'),
('L-010', 'APP-004', 'FinalDecision', 'Rejected', TIMESTAMP '2025-01-03 16:00:00', 'U-03'), -- Debt ratio
('L-011', 'APP-006', 'Received', 'Pass', TIMESTAMP '2025-01-03 11:00:00', 'SYS'),
('L-012', 'APP-006', 'DocReview', 'Pass', TIMESTAMP '2025-01-04 12:00:00', 'U-01'),
('L-013', 'APP-006', 'Appraisal', 'Fail', TIMESTAMP '2025-01-08 13:00:00', 'U-05'), -- Low appraisal
('L-014', 'APP-006', 'FinalDecision', 'Rejected', TIMESTAMP '2025-01-09 09:30:00', 'U-01'),
-- ... (Additional logs implied for coverage)
('L-050', 'APP-050', 'Received', 'Pass', TIMESTAMP '2025-01-25 10:00:00', 'SYS');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Pipeline View
-------------------------------------------------------------------------------
-- Description: Connecting applications to their latest status.
-- Transformation: Grouping logs to find the "Current Stage" of each loan.

CREATE OR REPLACE VIEW LendingDB.Silver.ApplicationStatusHistory AS
SELECT
    a.AppID,
    a.LoanAmount,
    a.PropertyState,
    a.ProductType,
    l.Stage AS CurrentStage,
    l.Status AS CurrentStatus,
    l.LogTime
FROM LendingDB.Bronze.Applications a
JOIN LendingDB.Bronze.UnderwriterLog l ON a.AppID = l.AppID
-- In a real scenario, we'd filter for the LATEST log entry per AppID
-- For this simplified demo, we show all flow events to visualize the history.
;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Funnel Analytics
-------------------------------------------------------------------------------
-- Description: Aggregating counts by Stage and Decision.
-- Insight: Where are loans getting stuck or rejected?

CREATE OR REPLACE VIEW LendingDB.Gold.ConversionRateByStage AS
SELECT
    Stage,
    Status,
    COUNT(DISTINCT AppID) AS ApplicationCount,
    SUM(LoanAmount) AS TotalVolume
FROM LendingDB.Silver.ApplicationStatusHistory
GROUP BY Stage, Status;

CREATE OR REPLACE VIEW LendingDB.Gold.ApprovalsByState AS
SELECT
    PropertyState,
    COUNT(DISTINCT AppID) AS ApprovedLoans,
    SUM(LoanAmount) AS ApprovedVolume
FROM LendingDB.Silver.ApplicationStatusHistory
WHERE CurrentStage = 'FinalDecision' AND CurrentStatus = 'Approved'
GROUP BY PropertyState;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Visualize the funnel drop-off using LendingDB.Gold.ConversionRateByStage, showing ApplicationCount by Stage."

PROMPT:
"Which PropertyState has the highest volume of approved loans in LendingDB.Gold.ApprovalsByState?"
*/
