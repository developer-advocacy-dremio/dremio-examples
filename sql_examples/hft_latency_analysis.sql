/*
 * HFT Latency Analysis Demo
 * 
 * Scenario:
 * A High-Frequency Trading firm analyzing the gap between "Order Sent" and "Ack Received" (Round-Trip Time).
 * 
 * Data Context:
 * - OrderLog: Microsecond-precision timestamps of outbound orders.
 * - AckLog: Microsecond-precision timestamps of exchange acknowledgments.
 * 
 * Analytical Goal:
 * Detect latency spikes > 100 microseconds that correlate with slippage.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HFT_OpsDB;
CREATE FOLDER IF NOT EXISTS HFT_OpsDB.Bronze;
CREATE FOLDER IF NOT EXISTS HFT_OpsDB.Silver;
CREATE FOLDER IF NOT EXISTS HFT_OpsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS HFT_OpsDB.Bronze.OrderLog (
    OrderID VARCHAR,
    Symbol VARCHAR,
    Exchange VARCHAR, -- 'NASDAQ', 'NYSE', 'IEX'
    TimeSent TIMESTAMP -- Precision assumed sufficient
);

CREATE TABLE IF NOT EXISTS HFT_OpsDB.Bronze.AckLog (
    AckID VARCHAR,
    OrderID VARCHAR,
    TimeReceived TIMESTAMP,
    Status VARCHAR -- 'Filled', 'Rejected'
);

INSERT INTO HFT_OpsDB.Bronze.OrderLog VALUES
('O-01', 'AAPL', 'NASDAQ', '2025-01-01 09:30:00.000000'),
('O-02', 'AAPL', 'NASDAQ', '2025-01-01 09:30:00.001000'),
('O-03', 'GOOG', 'NYSE', '2025-01-01 09:30:00.002000'),
('O-04', 'GOOG', 'NYSE', '2025-01-01 09:30:00.003000'),
('O-05', 'MSFT', 'IEX', '2025-01-01 09:30:01.000000'),
('O-06', 'MSFT', 'IEX', '2025-01-01 09:30:01.001000'),
('O-07', 'TSLA', 'NASDAQ', '2025-01-01 09:30:02.000000'),
('O-08', 'TSLA', 'NASDAQ', '2025-01-01 09:30:02.005000'),
('O-09', 'AMZN', 'NYSE', '2025-01-01 09:30:03.000000'),
('O-10', 'AMZN', 'NYSE', '2025-01-01 09:30:03.001000');

INSERT INTO HFT_OpsDB.Bronze.AckLog VALUES
('A-01', 'O-01', '2025-01-01 09:30:00.000050', 'Filled'), -- 50us
('A-02', 'O-02', '2025-01-01 09:30:00.001040', 'Filled'), -- 40us
('A-03', 'O-03', '2025-01-01 09:30:00.002150', 'Filled'), -- 150us (Spike)
('A-04', 'O-04', '2025-01-01 09:30:00.003060', 'Rejected'),
('A-05', 'O-05', '2025-01-01 09:30:01.000045', 'Filled'),
('A-06', 'O-06', '2025-01-01 09:30:01.001050', 'Filled'),
('A-07', 'O-07', '2025-01-01 09:30:02.000200', 'Filled'), -- 200us (Spike)
('A-08', 'O-08', '2025-01-01 09:30:02.005040', 'Filled'),
('A-09', 'O-09', '2025-01-01 09:30:03.000060', 'Filled'),
('A-10', 'O-10', '2025-01-01 09:30:03.001055', 'Filled');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HFT_OpsDB.Silver.LatencyMetrics AS
SELECT 
    o.Symbol,
    o.Exchange,
    o.TimeSent,
    a.TimeReceived,
    -- Calculate Latency in Microseconds (Mock math)
    -- In real SQL we might use DATEDIFF with high precision
    (EXTRACT(SECOND FROM a.TimeReceived) - EXTRACT(SECOND FROM o.TimeSent)) * 1000000 AS LatencyMicroseconds
FROM HFT_OpsDB.Bronze.OrderLog o
JOIN HFT_OpsDB.Bronze.AckLog a ON o.OrderID = a.OrderID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HFT_OpsDB.Gold.ExchangePerformance AS
SELECT 
    Exchange,
    COUNT(*) AS OrderCount,
    AVG(LatencyMicroseconds) AS AvgLatencyUs,
    MAX(LatencyMicroseconds) AS MaxLatencyUs,
    SUM(CASE WHEN LatencyMicroseconds > 100 THEN 1 ELSE 0 END) AS SlowOrders
FROM HFT_OpsDB.Silver.LatencyMetrics
GROUP BY Exchange;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Exchange has the highest MaxLatencyUs usage in HFT_OpsDB.Gold.ExchangePerformance?"

PROMPT:
"List all SlowOrders (Latency > 100us) details from HFT_OpsDB.Silver.LatencyMetrics sorted by TimeSent."
*/
