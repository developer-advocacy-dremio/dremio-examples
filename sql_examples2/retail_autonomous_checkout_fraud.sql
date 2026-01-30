/*
 * Retail: Autonomous Checkout Fraud Detection
 * 
 * Scenario:
 * Detecting anomalies in "Just Walk Out" computer vision sessions (item mismatches, weight discrepencies).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AutonomousRetailDB;
CREATE FOLDER IF NOT EXISTS AutonomousRetailDB.Operations;
CREATE FOLDER IF NOT EXISTS AutonomousRetailDB.Operations.Bronze;
CREATE FOLDER IF NOT EXISTS AutonomousRetailDB.Operations.Silver;
CREATE FOLDER IF NOT EXISTS AutonomousRetailDB.Operations.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Session Logs
-------------------------------------------------------------------------------

-- CheckoutSessions Table
CREATE TABLE IF NOT EXISTS AutonomousRetailDB.Operations.Bronze.CheckoutSessions (
    SessionID VARCHAR,
    StoreLocation VARCHAR,
    CustomerAppID VARCHAR,
    EnteredItemCount INT, -- Detected by entry gate cameras? Or items picked
    ExitWeightKg DOUBLE, -- Total weight at exit
    PaymentStatus VARCHAR, -- Success, Failed, Flagged
    SessionStartTimestamp TIMESTAMP,
    SessionEndTimestamp TIMESTAMP
);

INSERT INTO AutonomousRetailDB.Operations.Bronze.CheckoutSessions VALUES
('S-001', 'NYC-01', 'App-100', 5, 2.5, 'Success', '2026-11-01 10:00:00', '2026-11-01 10:05:00'),
('S-002', 'NYC-01', 'App-101', 3, 10.0, 'Success', '2026-11-01 10:10:00', '2026-11-01 10:20:00'), -- Heavy items?
('S-003', 'NYC-01', 'App-102', 2, 0.5, 'Success', '2026-11-01 10:15:00', '2026-11-01 10:18:00'),
('S-004', 'NYC-01', 'App-103', 10, 1.0, 'Flagged', '2026-11-01 10:20:00', '2026-11-01 10:25:00'), -- 10 items but only 1kg? Suspicious
('S-005', 'NYC-01', 'App-104', 4, 2.0, 'Success', '2026-11-01 10:30:00', '2026-11-01 10:35:00'),
('S-006', 'NYC-02', 'App-105', 6, 3.0, 'Success', '2026-11-01 10:00:00', '2026-11-01 10:10:00'),
('S-007', 'NYC-02', 'App-105', 2, 8.0, 'Flagged', '2026-11-01 12:00:00', '2026-11-01 12:05:00'), -- 2 items 8kg? Maybe water cases?
('S-008', 'NYC-01', 'App-100', 1, 0.2, 'Success', '2026-11-02 10:00:00', '2026-11-02 10:02:00'),
('S-009', 'NYC-01', 'App-106', 15, 6.0, 'Success', '2026-11-01 11:00:00', '2026-11-01 11:15:00'),
('S-010', 'NYC-01', 'App-107', 5, 2.0, 'Failed', '2026-11-01 11:20:00', '2026-11-01 11:25:00'),
('S-011', 'NYC-01', 'App-108', 3, 1.5, 'Success', '2026-11-01 11:30:00', '2026-11-01 11:35:00'),
('S-012', 'NYC-01', 'App-109', 7, 3.5, 'Success', '2026-11-01 11:40:00', '2026-11-01 11:45:00'),
('S-013', 'NYC-01', 'App-110', 9, 4.5, 'Success', '2026-11-01 11:50:00', '2026-11-01 11:55:00'),
('S-014', 'NYC-01', 'App-111', 2, 1.0, 'Success', '2026-11-01 12:00:00', '2026-11-01 12:05:00'),
('S-015', 'NYC-01', 'App-112', 4, 1.8, 'Success', '2026-11-01 12:10:00', '2026-11-01 12:15:00'),
('S-016', 'NYC-01', 'App-113', 6, 2.8, 'Success', '2026-11-01 12:20:00', '2026-11-01 12:25:00'),
('S-017', 'NYC-01', 'App-114', 8, 3.8, 'Success', '2026-11-01 12:30:00', '2026-11-01 12:35:00'),
('S-018', 'NYC-01', 'App-115', 10, 4.8, 'Success', '2026-11-01 12:40:00', '2026-11-01 12:45:00'),
('S-019', 'NYC-01', 'App-116', 3, 1.2, 'Success', '2026-11-01 12:50:00', '2026-11-01 12:55:00'),
('S-020', 'NYC-01', 'App-117', 5, 2.2, 'Success', '2026-11-01 13:00:00', '2026-11-01 13:05:00'),
('S-021', 'NYC-01', 'App-118', 7, 3.2, 'Success', '2026-11-01 13:10:00', '2026-11-01 13:15:00'),
('S-022', 'NYC-01', 'App-119', 9, 4.2, 'Success', '2026-11-01 13:20:00', '2026-11-01 13:25:00'),
('S-023', 'NYC-01', 'App-120', 2, 0.8, 'Success', '2026-11-01 13:30:00', '2026-11-01 13:35:00'),
('S-024', 'NYC-01', 'App-121', 4, 1.6, 'Success', '2026-11-01 13:40:00', '2026-11-01 13:45:00'),
('S-025', 'NYC-01', 'App-122', 6, 2.6, 'Success', '2026-11-01 13:50:00', '2026-11-01 13:55:00'),
('S-026', 'NYC-01', 'App-123', 8, 3.6, 'Success', '2026-11-01 14:00:00', '2026-11-01 14:05:00'),
('S-027', 'NYC-01', 'App-124', 10, 4.6, 'Success', '2026-11-01 14:10:00', '2026-11-01 14:15:00'),
('S-028', 'NYC-01', 'App-125', 3, 1.4, 'Success', '2026-11-01 14:20:00', '2026-11-01 14:25:00'),
('S-029', 'NYC-01', 'App-126', 5, 2.4, 'Success', '2026-11-01 14:30:00', '2026-11-01 14:35:00'),
('S-030', 'NYC-01', 'App-127', 7, 3.4, 'Success', '2026-11-01 14:40:00', '2026-11-01 14:45:00'),
('S-031', 'NYC-01', 'App-128', 9, 4.4, 'Success', '2026-11-01 14:50:00', '2026-11-01 14:55:00'),
('S-032', 'NYC-01', 'App-129', 2, 1.1, 'Success', '2026-11-01 15:00:00', '2026-11-01 15:05:00'),
('S-033', 'NYC-01', 'App-130', 4, 1.9, 'Success', '2026-11-01 15:10:00', '2026-11-01 15:15:00'),
('S-034', 'NYC-01', 'App-131', 6, 2.9, 'Success', '2026-11-01 15:20:00', '2026-11-01 15:25:00'),
('S-035', 'NYC-01', 'App-132', 8, 3.9, 'Success', '2026-11-01 15:30:00', '2026-11-01 15:35:00'),
('S-036', 'NYC-01', 'App-133', 10, 4.9, 'Success', '2026-11-01 15:40:00', '2026-11-01 15:45:00'),
('S-037', 'NYC-01', 'App-134', 3, 1.3, 'Success', '2026-11-01 15:50:00', '2026-11-01 15:55:00'),
('S-038', 'NYC-01', 'App-135', 5, 2.3, 'Success', '2026-11-01 16:00:00', '2026-11-01 16:05:00'),
('S-039', 'NYC-01', 'App-136', 7, 3.3, 'Success', '2026-11-01 16:10:00', '2026-11-01 16:15:00'),
('S-040', 'NYC-01', 'App-137', 9, 4.3, 'Success', '2026-11-01 16:20:00', '2026-11-01 16:25:00'),
('S-041', 'NYC-01', 'App-138', 2, 0.9, 'Success', '2026-11-01 16:30:00', '2026-11-01 16:35:00'),
('S-042', 'NYC-01', 'App-139', 4, 1.7, 'Success', '2026-11-01 16:40:00', '2026-11-01 16:45:00'),
('S-043', 'NYC-01', 'App-140', 6, 2.7, 'Success', '2026-11-01 16:50:00', '2026-11-01 16:55:00'),
('S-044', 'NYC-01', 'App-141', 8, 3.7, 'Success', '2026-11-01 17:00:00', '2026-11-01 17:05:00'),
('S-045', 'NYC-01', 'App-142', 10, 4.7, 'Success', '2026-11-01 17:10:00', '2026-11-01 17:15:00'),
('S-046', 'NYC-01', 'App-143', 3, 1.5, 'Success', '2026-11-01 17:20:00', '2026-11-01 17:25:00'),
('S-047', 'NYC-01', 'App-144', 5, 2.5, 'Success', '2026-11-01 17:30:00', '2026-11-01 17:35:00'),
('S-048', 'NYC-01', 'App-145', 7, 3.5, 'Success', '2026-11-01 17:40:00', '2026-11-01 17:45:00'),
('S-049', 'NYC-01', 'App-146', 9, 4.5, 'Success', '2026-11-01 17:50:00', '2026-11-01 17:55:00'),
('S-050', 'NYC-01', 'App-147', 12, 1.0, 'Flagged', '2026-11-01 18:00:00', '2026-11-01 18:05:00'); -- 12 items, 1kg

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Anomaly Scoring
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AutonomousRetailDB.Operations.Silver.SessionAnomalies AS
SELECT 
    SessionID,
    CustomerAppID,
    EnteredItemCount,
    ExitWeightKg,
    -- Heuristic: Avg weight per item should be reasonable (e.g., > 0.1kg and < 5kg??)
    ExitWeightKg / EnteredItemCount AS AvgItemWeight,
    PaymentStatus,
    -- Detect massive mismatch
    CASE 
        WHEN (ExitWeightKg / EnteredItemCount) < 0.1 THEN 'Low Weight Anomaly' -- Items concealed?
        WHEN (ExitWeightKg / EnteredItemCount) > 10.0 THEN 'High Weight Anomaly' -- Unscanned items?
        WHEN PaymentStatus = 'Flagged' THEN 'System Flag'
        ELSE 'Normal'
    END AS AnomalyType
FROM AutonomousRetailDB.Operations.Bronze.CheckoutSessions;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Loss Prevention
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AutonomousRetailDB.Operations.Gold.FraudAlerts AS
SELECT 
    SessionID,
    CustomerAppID,
    AnomalyType,
    AvgItemWeight,
    'Manual Review Required' AS Action
FROM AutonomousRetailDB.Operations.Silver.SessionAnomalies
WHERE AnomalyType != 'Normal';

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all sessions requiring 'Manual Review' in the Gold alerts view."

PROMPT 2:
"Count the number of 'Low Weight Anomaly' events detected."

PROMPT 3:
"Show the average ExitWeightKg for 'Normal' sessions."
*/
