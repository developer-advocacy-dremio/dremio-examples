/*
 * SWIFT Payment Tracking Demo
 * 
 * Scenario:
 * Operations teams track cross-border wire transfers (SWIFT MT103).
 * Key goal is to identify payments that haven't received a Final Acknowledgement (ACK) 
 * or have been NACKed (Rejected).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Alert on "Stuck" payments > 24 hours old.
 * 
 * Note: Assumes a catalog named 'PaymentsOpsDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS PaymentsOpsDB;
CREATE FOLDER IF NOT EXISTS PaymentsOpsDB.Bronze;
CREATE FOLDER IF NOT EXISTS PaymentsOpsDB.Silver;
CREATE FOLDER IF NOT EXISTS PaymentsOpsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Message Logs
-------------------------------------------------------------------------------
-- Description: Raw message logs from the SWIFT Gateway.

CREATE TABLE IF NOT EXISTS PaymentsOpsDB.Bronze.SwiftMessages (
    MsgID VARCHAR,
    SenderBIC VARCHAR,
    ReceiverBIC VARCHAR,
    ValueDate DATE,
    Amount DOUBLE,
    Currency VARCHAR,
    CreationTime TIMESTAMP
);

CREATE TABLE IF NOT EXISTS PaymentsOpsDB.Bronze.AckNackLogs (
    LogID INT,
    RefMsgID VARCHAR,
    Status VARCHAR, -- 'ACK', 'NACK'
    LogTime TIMESTAMP,
    ErrorCode VARCHAR
);

-- 1.1 Populate SwiftMessages (50+ Records)
INSERT INTO PaymentsOpsDB.Bronze.SwiftMessages (MsgID, SenderBIC, ReceiverBIC, ValueDate, Amount, Currency, CreationTime) VALUES
('SW-001', 'BANKUS33', 'BANKDEFF', '2025-01-20', 50000.00, 'USD', TIMESTAMP '2025-01-20 09:00:00'),
('SW-002', 'BANKUS33', 'BANKGB22', '2025-01-20', 12000.00, 'GBP', TIMESTAMP '2025-01-20 09:05:00'),
('SW-003', 'BANKUS33', 'BANKJPJP', '2025-01-20', 100000.00, 'JPY', TIMESTAMP '2025-01-20 09:10:00'), -- NACK
('SW-004', 'BANKUS33', 'BANKDEFF', '2025-01-20', 5500.00, 'EUR', TIMESTAMP '2025-01-20 09:15:00'),
('SW-005', 'BANKUS33', 'BANKCNCN', '2025-01-20', 80000.00, 'CNY', TIMESTAMP '2025-01-20 09:20:00'), -- Stuck (No Ack)
('SW-006', 'BANKUS33', 'BANKUS44', '2025-01-20', 500.00, 'USD', TIMESTAMP '2025-01-20 09:25:00'),
('SW-007', 'BANKUS33', 'BANKSGSG', '2025-01-20', 25000.00, 'SGD', TIMESTAMP '2025-01-20 09:30:00'),
('SW-008', 'BANKUS33', 'BANKAUAU', '2025-01-20', 15000.00, 'AUD', TIMESTAMP '2025-01-20 09:35:00'),
('SW-009', 'BANKUS33', 'BANKFRPP', '2025-01-20', 30000.00, 'EUR', TIMESTAMP '2025-01-20 09:40:00'),
('SW-010', 'BANKUS33', 'BANKDEFF', '2025-01-20', 1000.00, 'EUR', TIMESTAMP '2025-01-20 09:45:00'),
('SW-011', 'BANKUS33', 'BANKGB22', '2025-01-20', 2000.00, 'GBP', TIMESTAMP '2025-01-20 09:50:00'),
('SW-012', 'BANKUS33', 'BANKJPJP', '2025-01-20', 500000.00, 'JPY', TIMESTAMP '2025-01-20 09:55:00'),
('SW-013', 'BANKUS33', 'BANKCNCN', '2025-01-20', 10000.00, 'CNY', TIMESTAMP '2025-01-20 10:00:00'),
('SW-014', 'BANKUS33', 'BANKUS44', '2025-01-20', 300.00, 'USD', TIMESTAMP '2025-01-20 10:05:00'),
('SW-015', 'BANKUS33', 'BANKSGSG', '2025-01-20', 12000.00, 'SGD', TIMESTAMP '2025-01-20 10:10:00'),
('SW-016', 'BANKUS33', 'BANKAUAU', '2025-01-20', 5000.00, 'AUD', TIMESTAMP '2025-01-20 10:15:00'),
('SW-017', 'BANKUS33', 'BANKFRPP', '2025-01-20', 8000.00, 'EUR', TIMESTAMP '2025-01-20 10:20:00'),
('SW-018', 'BANKUS33', 'BANKDEFF', '2025-01-20', 4000.00, 'EUR', TIMESTAMP '2025-01-20 10:25:00'),
('SW-019', 'BANKUS33', 'BANKGB22', '2025-01-20', 10000.00, 'GBP', TIMESTAMP '2025-01-20 10:30:00'),
('SW-020', 'BANKUS33', 'BANKJPJP', '2025-01-20', 200000.00, 'JPY', TIMESTAMP '2025-01-20 10:35:00'), -- NACK
('SW-021', 'BANKUS33', 'BANKCNCN', '2025-01-20', 50000.00, 'CNY', TIMESTAMP '2025-01-20 10:40:00'),
('SW-022', 'BANKUS33', 'BANKUS44', '2025-01-20', 1500.00, 'USD', TIMESTAMP '2025-01-20 10:45:00'),
('SW-023', 'BANKUS33', 'BANKSGSG', '2025-01-20', 30000.00, 'SGD', TIMESTAMP '2025-01-20 10:50:00'), -- Stuck
('SW-024', 'BANKUS33', 'BANKAUAU', '2025-01-20', 20000.00, 'AUD', TIMESTAMP '2025-01-20 10:55:00'),
('SW-025', 'BANKUS33', 'BANKFRPP', '2025-01-20', 15000.00, 'EUR', TIMESTAMP '2025-01-20 11:00:00'),
('SW-026', 'BANKUS33', 'BANKDEFF', '2025-01-20', 9000.00, 'EUR', TIMESTAMP '2025-01-20 11:05:00'),
('SW-027', 'BANKUS33', 'BANKGB22', '2025-01-20', 6000.00, 'GBP', TIMESTAMP '2025-01-20 11:10:00'),
('SW-028', 'BANKUS33', 'BANKJPJP', '2025-01-20', 40000.00, 'JPY', TIMESTAMP '2025-01-20 11:15:00'),
('SW-029', 'BANKUS33', 'BANKCNCN', '2025-01-20', 7000.00, 'CNY', TIMESTAMP '2025-01-20 11:20:00'),
('SW-030', 'BANKUS33', 'BANKUS44', '2025-01-20', 800.00, 'USD', TIMESTAMP '2025-01-20 11:25:00'),
('SW-031', 'BANKUS33', 'BANKSGSG', '2025-01-20', 18000.00, 'SGD', TIMESTAMP '2025-01-20 11:30:00'),
('SW-032', 'BANKUS33', 'BANKAUAU', '2025-01-20', 12000.00, 'AUD', TIMESTAMP '2025-01-20 11:35:00'),
('SW-033', 'BANKUS33', 'BANKFRPP', '2025-01-20', 22000.00, 'EUR', TIMESTAMP '2025-01-20 11:40:00'),
('SW-034', 'BANKUS33', 'BANKDEFF', '2025-01-20', 3000.00, 'EUR', TIMESTAMP '2025-01-20 11:45:00'),
('SW-035', 'BANKUS33', 'BANKGB22', '2025-01-20', 4000.00, 'GBP', TIMESTAMP '2025-01-20 11:50:00'),
('SW-036', 'BANKUS33', 'BANKJPJP', '2025-01-20', 150000.00, 'JPY', TIMESTAMP '2025-01-20 11:55:00'),
('SW-037', 'BANKUS33', 'BANKCNCN', '2025-01-20', 12000.00, 'CNY', TIMESTAMP '2025-01-20 12:00:00'),
('SW-038', 'BANKUS33', 'BANKUS44', '2025-01-20', 2500.00, 'USD', TIMESTAMP '2025-01-20 12:05:00'),
('SW-039', 'BANKUS33', 'BANKSGSG', '2025-01-20', 15000.00, 'SGD', TIMESTAMP '2025-01-20 12:10:00'),
('SW-040', 'BANKUS33', 'BANKAUAU', '2025-01-20', 8000.00, 'AUD', TIMESTAMP '2025-01-20 12:15:00'),
('SW-041', 'BANKUS33', 'BANKFRPP', '2025-01-20', 25000.00, 'EUR', TIMESTAMP '2025-01-20 12:20:00'), -- Stuck
('SW-042', 'BANKUS33', 'BANKDEFF', '2025-01-20', 1500.00, 'EUR', TIMESTAMP '2025-01-20 12:25:00'),
('SW-043', 'BANKUS33', 'BANKGB22', '2025-01-20', 3500.00, 'GBP', TIMESTAMP '2025-01-20 12:30:00'),
('SW-044', 'BANKUS33', 'BANKJPJP', '2025-01-20', 80000.00, 'JPY', TIMESTAMP '2025-01-20 12:35:00'),
('SW-045', 'BANKUS33', 'BANKCNCN', '2025-01-20', 15000.00, 'CNY', TIMESTAMP '2025-01-20 12:40:00'),
('SW-046', 'BANKUS33', 'BANKUS44', '2025-01-20', 1000.00, 'USD', TIMESTAMP '2025-01-20 12:45:00'),
('SW-047', 'BANKUS33', 'BANKSGSG', '2025-01-20', 20000.00, 'SGD', TIMESTAMP '2025-01-20 12:50:00'),
('SW-048', 'BANKUS33', 'BANKAUAU', '2025-01-20', 11000.00, 'AUD', TIMESTAMP '2025-01-20 12:55:00'),
('SW-049', 'BANKUS33', 'BANKFRPP', '2025-01-20', 10000.00, 'EUR', TIMESTAMP '2025-01-20 13:00:00'),
('SW-050', 'BANKUS33', 'BANKDEFF', '2025-01-20', 2500.00, 'EUR', TIMESTAMP '2025-01-20 13:05:00');

-- 1.2 Populate AckNackLogs (Missing some to simulate Stuck)
INSERT INTO PaymentsOpsDB.Bronze.AckNackLogs (LogID, RefMsgID, Status, LogTime, ErrorCode) VALUES
(1, 'SW-001', 'ACK', TIMESTAMP '2025-01-20 09:00:05', 'OK'),
(2, 'SW-002', 'ACK', TIMESTAMP '2025-01-20 09:05:05', 'OK'),
(3, 'SW-003', 'NACK', TIMESTAMP '2025-01-20 09:10:05', 'Invalid IBAN'),
(4, 'SW-004', 'ACK', TIMESTAMP '2025-01-20 09:15:05', 'OK'),
-- SW-005 is stuck (Missing)
(5, 'SW-006', 'ACK', TIMESTAMP '2025-01-20 09:25:05', 'OK'),
(6, 'SW-007', 'ACK', TIMESTAMP '2025-01-20 09:30:05', 'OK'),
(7, 'SW-008', 'ACK', TIMESTAMP '2025-01-20 09:35:05', 'OK'),
(8, 'SW-009', 'ACK', TIMESTAMP '2025-01-20 09:40:05', 'OK'),
(9, 'SW-010', 'ACK', TIMESTAMP '2025-01-20 09:45:05', 'OK'),
(10, 'SW-011', 'ACK', TIMESTAMP '2025-01-20 09:50:05', 'OK'),
(11, 'SW-012', 'ACK', TIMESTAMP '2025-01-20 09:55:05', 'OK'),
(12, 'SW-013', 'ACK', TIMESTAMP '2025-01-20 10:00:05', 'OK'),
(13, 'SW-014', 'ACK', TIMESTAMP '2025-01-20 10:05:05', 'OK'),
(14, 'SW-015', 'ACK', TIMESTAMP '2025-01-20 10:10:05', 'OK'),
(15, 'SW-016', 'ACK', TIMESTAMP '2025-01-20 10:15:05', 'OK'),
(16, 'SW-017', 'ACK', TIMESTAMP '2025-01-20 10:20:05', 'OK'),
(17, 'SW-018', 'ACK', TIMESTAMP '2025-01-20 10:25:05', 'OK'),
(18, 'SW-019', 'ACK', TIMESTAMP '2025-01-20 10:30:05', 'OK'),
(19, 'SW-020', 'NACK', TIMESTAMP '2025-01-20 10:35:05', 'Sanction Check'),
(20, 'SW-021', 'ACK', TIMESTAMP '2025-01-20 10:40:05', 'OK'),
(21, 'SW-022', 'ACK', TIMESTAMP '2025-01-20 10:45:05', 'OK'),
-- SW-023 is stuck
(22, 'SW-024', 'ACK', TIMESTAMP '2025-01-20 10:55:05', 'OK'),
(23, 'SW-025', 'ACK', TIMESTAMP '2025-01-20 11:00:05', 'OK'),
(24, 'SW-026', 'ACK', TIMESTAMP '2025-01-20 11:05:05', 'OK'),
(25, 'SW-027', 'ACK', TIMESTAMP '2025-01-20 11:10:05', 'OK'),
(26, 'SW-028', 'ACK', TIMESTAMP '2025-01-20 11:15:05', 'OK'),
(27, 'SW-029', 'ACK', TIMESTAMP '2025-01-20 11:20:05', 'OK'),
(28, 'SW-030', 'ACK', TIMESTAMP '2025-01-20 11:25:05', 'OK'),
(29, 'SW-031', 'ACK', TIMESTAMP '2025-01-20 11:30:05', 'OK'),
(30, 'SW-032', 'ACK', TIMESTAMP '2025-01-20 11:35:05', 'OK'),
(31, 'SW-033', 'ACK', TIMESTAMP '2025-01-20 11:40:05', 'OK'),
(32, 'SW-034', 'ACK', TIMESTAMP '2025-01-20 11:45:05', 'OK'),
(33, 'SW-035', 'ACK', TIMESTAMP '2025-01-20 11:50:05', 'OK'),
(34, 'SW-036', 'ACK', TIMESTAMP '2025-01-20 11:55:05', 'OK'),
(35, 'SW-037', 'ACK', TIMESTAMP '2025-01-20 12:00:05', 'OK'),
(36, 'SW-038', 'ACK', TIMESTAMP '2025-01-20 12:05:05', 'OK'),
(37, 'SW-039', 'ACK', TIMESTAMP '2025-01-20 12:10:05', 'OK'),
(38, 'SW-040', 'ACK', TIMESTAMP '2025-01-20 12:15:05', 'OK'),
-- SW-041 is stuck
(39, 'SW-042', 'ACK', TIMESTAMP '2025-01-20 12:25:05', 'OK'),
(40, 'SW-043', 'ACK', TIMESTAMP '2025-01-20 12:30:05', 'OK'),
(41, 'SW-044', 'ACK', TIMESTAMP '2025-01-20 12:35:05', 'OK'),
(42, 'SW-045', 'ACK', TIMESTAMP '2025-01-20 12:40:05', 'OK'),
(43, 'SW-046', 'ACK', TIMESTAMP '2025-01-20 12:45:05', 'OK'),
(44, 'SW-047', 'ACK', TIMESTAMP '2025-01-20 12:50:05', 'OK'),
(45, 'SW-048', 'ACK', TIMESTAMP '2025-01-20 12:55:05', 'OK'),
(46, 'SW-049', 'ACK', TIMESTAMP '2025-01-20 13:00:05', 'OK'),
(47, 'SW-050', 'ACK', TIMESTAMP '2025-01-20 13:05:05', 'OK');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Correlations
-------------------------------------------------------------------------------
-- Description: Combining messages with their logs.
-- Logic: Left join. If status is NULL, it's pending/stuck.

CREATE OR REPLACE VIEW PaymentsOpsDB.Silver.PaymentStatus AS
SELECT
    m.MsgID,
    m.ReceiverBIC,
    m.Amount,
    m.Currency,
    m.CreationTime,
    COALESCE(l.Status, 'PENDING') AS AcknowledgementStatus,
    l.LogTime
FROM PaymentsOpsDB.Bronze.SwiftMessages m
LEFT JOIN PaymentsOpsDB.Bronze.AckNackLogs l ON m.MsgID = l.RefMsgID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Exceptions Reporting
-------------------------------------------------------------------------------
-- Description: Identifying stuck payments.
-- Logic: Pending > 1 hour (usually 30mins is concern in fast systems, day for slow).
-- We'll assume analyzing "now" is 2025-01-20 14:00:00.

CREATE OR REPLACE VIEW PaymentsOpsDB.Gold.StuckPaymentsReport AS
SELECT
    MsgID,
    ReceiverBIC,
    Amount,
    Currency,
    TIMESTAMPDIFF(MINUTE, CreationTime, TIMESTAMP '2025-01-20 14:00:00') AS MinutesPending
FROM PaymentsOpsDB.Silver.PaymentStatus
WHERE AcknowledgementStatus = 'PENDING';

CREATE OR REPLACE VIEW PaymentsOpsDB.Gold.InvestigationQueue AS
SELECT * 
FROM PaymentsOpsDB.Silver.PaymentStatus
WHERE AcknowledgementStatus = 'NACK'
   OR (AcknowledgementStatus = 'PENDING' AND TIMESTAMPDIFF(MINUTE, CreationTime, TIMESTAMP '2025-01-20 14:00:00') > 60);

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all stuck payments from PaymentsOpsDB.Gold.StuckPaymentsReport ordered by Amount descending."

PROMPT:
"Show the breakdown of AcknowledgementStatus in PaymentsOpsDB.Silver.PaymentStatus."
*/
