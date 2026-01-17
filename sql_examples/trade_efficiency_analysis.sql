/*
 * Trade Efficiency Analysis & STP Optimization Demo
 * 
 * Scenario:
 * A global investment bank wants to improve its Straight-Through Processing (STP) rates.
 * STP means trades flow from execution to settlement without manual intervention.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify trading desks with high manual touchpoints and optimize efficiency.
 * 
 * Note: Assumes a catalog named 'TradeOpsDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS TradeOpsDB;
CREATE FOLDER IF NOT EXISTS TradeOpsDB.Bronze;
CREATE FOLDER IF NOT EXISTS TradeOpsDB.Silver;
CREATE FOLDER IF NOT EXISTS TradeOpsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Instruction Data
-------------------------------------------------------------------------------
-- Description: Ingesting raw trade instructions from the Order Management System (OMS) 
-- and settlement acknowledgements from the custodian.
-- Use Case: Audit trail of every trade message sent and received.

CREATE TABLE IF NOT EXISTS TradeOpsDB.Bronze.TradeInstructions (
    TradeID VARCHAR,
    Desk VARCHAR,
    InstrumentType VARCHAR, -- 'Equity', 'Bond', 'Derivative'
    Quantity INT,
    Price DOUBLE,
    InstructionTime TIMESTAMP,
    IsManualEntry BOOLEAN
);

CREATE TABLE IF NOT EXISTS TradeOpsDB.Bronze.SettlementAcknowledgements (
    AckID VARCHAR,
    TradeID VARCHAR,
    Status VARCHAR, -- 'ACK', 'NACK' (Negative Acknowledgement)
    AckTime TIMESTAMP,
    Reason VARCHAR
);

-- 1.1 Populate TradeInstructions (50+ Records)
INSERT INTO TradeOpsDB.Bronze.TradeInstructions (TradeID, Desk, InstrumentType, Quantity, Price, InstructionTime, IsManualEntry) VALUES
('T-1001', 'Equities_NA', 'Equity', 100, 150.50, TIMESTAMP '2025-01-10 09:30:00', false),
('T-1002', 'Equities_NA', 'Equity', 500, 151.00, TIMESTAMP '2025-01-10 09:31:00', false),
('T-1003', 'Equities_EU', 'Equity', 200, 45.20, TIMESTAMP '2025-01-10 09:32:00', true), -- Manual
('T-1004', 'FixedIncome', 'Bond', 1000, 1002.50, TIMESTAMP '2025-01-10 09:35:00', false),
('T-1005', 'Derivatives', 'Option', 50, 5.00, TIMESTAMP '2025-01-10 09:40:00', true), -- Manual
('T-1006', 'Equities_NA', 'Equity', 150, 149.00, TIMESTAMP '2025-01-10 09:42:00', false),
('T-1007', 'Equities_EU', 'Equity', 300, 46.00, TIMESTAMP '2025-01-10 09:45:00', false),
('T-1008', 'FixedIncome', 'Bond', 2000, 998.00, TIMESTAMP '2025-01-10 09:50:00', false),
('T-1009', 'Derivatives', 'Future', 10, 3500.00, TIMESTAMP '2025-01-10 09:55:00', false),
('T-1010', 'Equities_NA', 'Equity', 100, 152.00, TIMESTAMP '2025-01-10 10:00:00', false),
('T-1011', 'Equities_NA', 'Equity', 600, 153.00, TIMESTAMP '2025-01-10 10:05:00', true),
('T-1012', 'Equities_EU', 'Equity', 250, 44.80, TIMESTAMP '2025-01-10 10:10:00', false),
('T-1013', 'FixedIncome', 'Bond', 1500, 1001.00, TIMESTAMP '2025-01-10 10:15:00', false),
('T-1014', 'Derivatives', 'Option', 60, 4.50, TIMESTAMP '2025-01-10 10:20:00', false),
('T-1015', 'Equities_NA', 'Equity', 200, 155.00, TIMESTAMP '2025-01-10 10:25:00', false),
('T-1016', 'Equities_EU', 'Equity', 400, 47.00, TIMESTAMP '2025-01-10 10:30:00', true),
('T-1017', 'FixedIncome', 'Bond', 3000, 999.00, TIMESTAMP '2025-01-10 10:35:00', false),
('T-1018', 'Derivatives', 'Future', 15, 3510.00, TIMESTAMP '2025-01-10 10:40:00', true),
('T-1019', 'Equities_NA', 'Equity', 120, 154.00, TIMESTAMP '2025-01-10 10:45:00', false),
('T-1020', 'Equities_NA', 'Equity', 700, 156.00, TIMESTAMP '2025-01-10 10:50:00', false),
('T-1021', 'Equities_EU', 'Equity', 350, 45.50, TIMESTAMP '2025-01-10 10:55:00', false),
('T-1022', 'FixedIncome', 'Bond', 2500, 1005.00, TIMESTAMP '2025-01-10 11:00:00', false),
('T-1023', 'Derivatives', 'Option', 70, 5.20, TIMESTAMP '2025-01-10 11:05:00', false),
('T-1024', 'Equities_NA', 'Equity', 180, 151.50, TIMESTAMP '2025-01-10 11:10:00', true),
('T-1025', 'Equities_EU', 'Equity', 450, 46.50, TIMESTAMP '2025-01-10 11:15:00', false),
('T-1026', 'FixedIncome', 'Bond', 4000, 997.00, TIMESTAMP '2025-01-10 11:20:00', false),
('T-1027', 'Derivatives', 'Future', 20, 3520.00, TIMESTAMP '2025-01-10 11:25:00', false),
('T-1028', 'Equities_NA', 'Equity', 130, 150.00, TIMESTAMP '2025-01-10 11:30:00', false),
('T-1029', 'Equities_NA', 'Equity', 800, 157.00, TIMESTAMP '2025-01-10 11:35:00', false),
('T-1030', 'Equities_EU', 'Equity', 500, 48.00, TIMESTAMP '2025-01-10 11:40:00', true),
('T-1031', 'FixedIncome', 'Bond', 3500, 1003.00, TIMESTAMP '2025-01-10 11:45:00', false),
('T-1032', 'Derivatives', 'Option', 80, 4.80, TIMESTAMP '2025-01-10 11:50:00', false),
('T-1033', 'Equities_NA', 'Equity', 220, 158.00, TIMESTAMP '2025-01-10 11:55:00', false),
('T-1034', 'Equities_EU', 'Equity', 550, 49.00, TIMESTAMP '2025-01-10 12:00:00', false),
('T-1035', 'FixedIncome', 'Bond', 4500, 996.00, TIMESTAMP '2025-01-10 12:05:00', true),
('T-1036', 'Derivatives', 'Future', 25, 3530.00, TIMESTAMP '2025-01-10 12:10:00', false),
('T-1037', 'Equities_NA', 'Equity', 140, 148.00, TIMESTAMP '2025-01-10 12:15:00', false),
('T-1038', 'Equities_NA', 'Equity', 900, 159.00, TIMESTAMP '2025-01-10 12:20:00', false),
('T-1039', 'Equities_EU', 'Equity', 600, 50.00, TIMESTAMP '2025-01-10 12:25:00', false),
('T-1040', 'FixedIncome', 'Bond', 5000, 1004.00, TIMESTAMP '2025-01-10 12:30:00', false),
('T-1041', 'Derivatives', 'Option', 90, 5.50, TIMESTAMP '2025-01-10 12:35:00', true),
('T-1042', 'Equities_NA', 'Equity', 250, 160.00, TIMESTAMP '2025-01-10 12:40:00', false),
('T-1043', 'Equities_EU', 'Equity', 650, 51.00, TIMESTAMP '2025-01-10 12:45:00', false),
('T-1044', 'FixedIncome', 'Bond', 5500, 995.00, TIMESTAMP '2025-01-10 12:50:00', false),
('T-1045', 'Derivatives', 'Future', 30, 3540.00, TIMESTAMP '2025-01-10 12:55:00', false),
('T-1046', 'Equities_NA', 'Equity', 160, 147.00, TIMESTAMP '2025-01-10 13:00:00', false),
('T-1047', 'Equities_NA', 'Equity', 1000, 161.00, TIMESTAMP '2025-01-10 13:05:00', true),
('T-1048', 'Equities_EU', 'Equity', 700, 52.00, TIMESTAMP '2025-01-10 13:10:00', false),
('T-1049', 'FixedIncome', 'Bond', 6000, 1006.00, TIMESTAMP '2025-01-10 13:15:00', false),
('T-1050', 'Derivatives', 'Option', 100, 5.80, TIMESTAMP '2025-01-10 13:20:00', false);

-- 1.2 Populate SettlementAcknowledgements (50+ Records)
INSERT INTO TradeOpsDB.Bronze.SettlementAcknowledgements (AckID, TradeID, Status, AckTime, Reason) VALUES
('A-001', 'T-1001', 'ACK', TIMESTAMP '2025-01-10 09:30:05', 'OK'),
('A-002', 'T-1002', 'ACK', TIMESTAMP '2025-01-10 09:31:05', 'OK'),
('A-003', 'T-1003', 'NACK', TIMESTAMP '2025-01-10 09:32:10', 'Invalid Account'), -- Fail
('A-004', 'T-1004', 'ACK', TIMESTAMP '2025-01-10 09:35:05', 'OK'),
('A-005', 'T-1005', 'ACK', TIMESTAMP '2025-01-10 09:40:05', 'OK'),
('A-006', 'T-1006', 'ACK', TIMESTAMP '2025-01-10 09:42:05', 'OK'),
('A-007', 'T-1007', 'ACK', TIMESTAMP '2025-01-10 09:45:05', 'OK'),
('A-008', 'T-1008', 'ACK', TIMESTAMP '2025-01-10 09:50:05', 'OK'),
('A-009', 'T-1009', 'ACK', TIMESTAMP '2025-01-10 09:55:05', 'OK'),
('A-010', 'T-1010', 'ACK', TIMESTAMP '2025-01-10 10:00:05', 'OK'),
('A-011', 'T-1011', 'ACK', TIMESTAMP '2025-01-10 10:05:05', 'OK'),
('A-012', 'T-1012', 'ACK', TIMESTAMP '2025-01-10 10:10:05', 'OK'),
('A-013', 'T-1013', 'ACK', TIMESTAMP '2025-01-10 10:15:05', 'OK'),
('A-014', 'T-1014', 'ACK', TIMESTAMP '2025-01-10 10:20:05', 'OK'),
('A-015', 'T-1015', 'ACK', TIMESTAMP '2025-01-10 10:25:05', 'OK'),
('A-016', 'T-1016', 'NACK', TIMESTAMP '2025-01-10 10:30:10', 'SSI Mismatch'), -- Fail
('A-017', 'T-1017', 'ACK', TIMESTAMP '2025-01-10 10:35:05', 'OK'),
('A-018', 'T-1018', 'ACK', TIMESTAMP '2025-01-10 10:40:05', 'OK'),
('A-019', 'T-1019', 'ACK', TIMESTAMP '2025-01-10 10:45:05', 'OK'),
('A-020', 'T-1020', 'ACK', TIMESTAMP '2025-01-10 10:50:05', 'OK'),
('A-021', 'T-1021', 'ACK', TIMESTAMP '2025-01-10 10:55:05', 'OK'),
('A-022', 'T-1022', 'ACK', TIMESTAMP '2025-01-10 11:00:05', 'OK'),
('A-023', 'T-1023', 'ACK', TIMESTAMP '2025-01-10 11:05:05', 'OK'),
('A-024', 'T-1024', 'ACK', TIMESTAMP '2025-01-10 11:10:05', 'OK'),
('A-025', 'T-1025', 'ACK', TIMESTAMP '2025-01-10 11:15:05', 'OK'),
('A-026', 'T-1026', 'ACK', TIMESTAMP '2025-01-10 11:20:05', 'OK'),
('A-027', 'T-1027', 'ACK', TIMESTAMP '2025-01-10 11:25:05', 'OK'),
('A-028', 'T-1028', 'ACK', TIMESTAMP '2025-01-10 11:30:05', 'OK'),
('A-029', 'T-1029', 'ACK', TIMESTAMP '2025-01-10 11:35:05', 'OK'),
('A-030', 'T-1030', 'NACK', TIMESTAMP '2025-01-10 11:40:10', 'Price Tolerance'), -- Fail
('A-031', 'T-1031', 'ACK', TIMESTAMP '2025-01-10 11:45:05', 'OK'),
('A-032', 'T-1032', 'ACK', TIMESTAMP '2025-01-10 11:50:05', 'OK'),
('A-033', 'T-1033', 'ACK', TIMESTAMP '2025-01-10 11:55:05', 'OK'),
('A-034', 'T-1034', 'ACK', TIMESTAMP '2025-01-10 12:00:05', 'OK'),
('A-035', 'T-1035', 'ACK', TIMESTAMP '2025-01-10 12:05:05', 'OK'),
('A-036', 'T-1036', 'ACK', TIMESTAMP '2025-01-10 12:10:05', 'OK'),
('A-037', 'T-1037', 'ACK', TIMESTAMP '2025-01-10 12:15:05', 'OK'),
('A-038', 'T-1038', 'ACK', TIMESTAMP '2025-01-10 12:20:05', 'OK'),
('A-039', 'T-1039', 'ACK', TIMESTAMP '2025-01-10 12:25:05', 'OK'),
('A-040', 'T-1040', 'ACK', TIMESTAMP '2025-01-10 12:30:05', 'OK'),
('A-041', 'T-1041', 'NACK', TIMESTAMP '2025-01-10 12:35:10', 'Unknown Symbol'), -- Fail
('A-042', 'T-1042', 'ACK', TIMESTAMP '2025-01-10 12:40:05', 'OK'),
('A-043', 'T-1043', 'ACK', TIMESTAMP '2025-01-10 12:45:05', 'OK'),
('A-044', 'T-1044', 'ACK', TIMESTAMP '2025-01-10 12:50:05', 'OK'),
('A-045', 'T-1045', 'ACK', TIMESTAMP '2025-01-10 12:55:05', 'OK'),
('A-046', 'T-1046', 'ACK', TIMESTAMP '2025-01-10 13:00:05', 'OK'),
('A-047', 'T-1047', 'NACK', TIMESTAMP '2025-01-10 13:05:10', 'Qty Mismatch'), -- Fail
('A-048', 'T-1048', 'ACK', TIMESTAMP '2025-01-10 13:10:05', 'OK'),
('A-049', 'T-1049', 'ACK', TIMESTAMP '2025-01-10 13:15:05', 'OK'),
('A-050', 'T-1050', 'ACK', TIMESTAMP '2025-01-10 13:20:05', 'OK');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Event Correlation
-------------------------------------------------------------------------------
-- Description: Combining instructions with their ack status to determine STP.
-- Transformation: Calculating time-to-ack and flagging failures.

CREATE OR REPLACE VIEW TradeOpsDB.Silver.TradeLifecycleEvents AS
SELECT
    t.TradeID,
    t.Desk,
    t.InstrumentType,
    t.Quantity,
    t.InstructionTime,
    t.IsManualEntry,
    a.Status AS AckStatus,
    a.Reason AS FailureReason,
    a.AckTime,
    CASE 
        WHEN t.IsManualEntry = false AND a.Status = 'ACK' THEN true 
        ELSE false 
    END AS IsSTP,
    TIMESTAMPDIFF(SECOND, t.InstructionTime, a.AckTime) AS LatencySeconds
FROM TradeOpsDB.Bronze.TradeInstructions t
LEFT JOIN TradeOpsDB.Bronze.SettlementAcknowledgements a ON t.TradeID = a.TradeID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Efficiency Metrics
-------------------------------------------------------------------------------
-- Description: Aggregating STP performance by trading desk.
-- Insight: Identifying which desks have the lowest automation rates and why.

CREATE OR REPLACE VIEW TradeOpsDB.Gold.STP_Rate_By_Desk AS
SELECT
    Desk,
    COUNT(TradeID) AS TotalTrades,
    SUM(CASE WHEN IsSTP = true THEN 1 ELSE 0 END) AS STP_Trades,
    (CAST(SUM(CASE WHEN IsSTP = true THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(TradeID)) * 100 AS STP_Percentage,
    AVG(LatencySeconds) AS AvgAckLatency
FROM TradeOpsDB.Silver.TradeLifecycleEvents
GROUP BY Desk;

CREATE OR REPLACE VIEW TradeOpsDB.Gold.FailureAnalysis AS
SELECT
    Desk,
    FailureReason,
    COUNT(*) AS FailCount
FROM TradeOpsDB.Silver.TradeLifecycleEvents
WHERE AckStatus = 'NACK'
GROUP BY Desk, FailureReason;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Visualize the STP_Percentage by Desk using TradeOpsDB.Gold.STP_Rate_By_Desk to see which team needs automation improvements."

PROMPT:
"List the top FailureReasons for the 'Equities_EU' desk from TradeOpsDB.Gold.FailureAnalysis."
*/
