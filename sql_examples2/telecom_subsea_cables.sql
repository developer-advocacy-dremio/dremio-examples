/*
 * Telecom: Subsea Cable Fault Localization
 * 
 * Scenario:
 * Analyzing optical time-domain reflectometry (OTDR) data to pinpoint signal degradation in trans-oceanic fiber cables.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SubseaTelcoDB;
CREATE FOLDER IF NOT EXISTS SubseaTelcoDB.Infrastructure;
CREATE FOLDER IF NOT EXISTS SubseaTelcoDB.Infrastructure.Bronze;
CREATE FOLDER IF NOT EXISTS SubseaTelcoDB.Infrastructure.Silver;
CREATE FOLDER IF NOT EXISTS SubseaTelcoDB.Infrastructure.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: OTDR Sensor Data
-------------------------------------------------------------------------------

-- CableReadings Table
CREATE TABLE IF NOT EXISTS SubseaTelcoDB.Infrastructure.Bronze.CableReadings (
    CableID VARCHAR, -- Atlantic-1, Pacific-Link
    SegmentID INT, -- 100km segments
    SignalLossDB DOUBLE, -- Decibels
    LatencyMs DOUBLE,
    Status VARCHAR, -- Active, Degraded, Broken
    ReadingTimestamp TIMESTAMP
);

INSERT INTO SubseaTelcoDB.Infrastructure.Bronze.CableReadings VALUES
('ATL-1', 1, 0.5, 5.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 2, 0.6, 10.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 3, 4.5, 12.0, 'Degraded', '2026-01-01 10:00:00'), -- Incident
('ATL-1', 4, 0.5, 15.0, 'Active', '2026-01-01 10:00:00'),
('PAC-1', 1, 0.4, 8.0, 'Active', '2026-01-01 10:00:00'),
('PAC-1', 2, 0.4, 16.0, 'Active', '2026-01-01 10:00:00'),
('PAC-1', 3, 0.5, 24.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 3, 6.0, 14.0, 'Degraded', '2026-01-01 11:00:00'), -- Worsening
('ATL-2', 1, 0.2, 4.0, 'Active', '2026-01-01 10:00:00'),
('ATL-2', 2, 0.3, 8.0, 'Active', '2026-01-01 10:00:00'),
('ATL-2', 3, 0.3, 12.0, 'Active', '2026-01-01 10:00:00'),
('IND-1', 1, 0.8, 6.0, 'Active', '2026-01-01 10:00:00'),
('IND-1', 2, 0.9, 12.0, 'Active', '2026-01-01 10:00:00'),
('IND-1', 3, 15.0, 999.0, 'Broken', '2026-01-01 10:00:00'), -- Cut
('IND-1', 4, 15.0, 999.0, 'Broken', '2026-01-01 10:00:00'),
('PAC-2', 1, 0.5, 7.0, 'Active', '2026-01-01 10:00:00'),
('PAC-2', 2, 0.5, 14.0, 'Active', '2026-01-01 10:00:00'),
('PAC-2', 3, 0.6, 21.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 5, 0.5, 20.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 6, 0.5, 25.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 7, 0.5, 30.0, 'Active', '2026-01-01 10:00:00'),
('PAC-1', 4, 0.5, 32.0, 'Active', '2026-01-01 10:00:00'),
('PAC-1', 5, 0.5, 40.0, 'Active', '2026-01-01 10:00:00'),
('ATL-2', 4, 0.3, 16.0, 'Active', '2026-01-01 10:00:00'),
('ATL-2', 5, 0.3, 20.0, 'Active', '2026-01-01 10:00:00'),
('IND-1', 5, 15.0, 999.0, 'Broken', '2026-01-01 10:00:00'),
('IND-1', 6, 15.0, 999.0, 'Broken', '2026-01-01 10:00:00'),
('PAC-2', 4, 0.6, 28.0, 'Active', '2026-01-01 10:00:00'),
('PAC-2', 5, 0.6, 35.0, 'Active', '2026-01-01 10:00:00'),
('MED-1', 1, 0.4, 5.0, 'Active', '2026-01-01 10:00:00'),
('MED-1', 2, 3.5, 10.0, 'Degraded', '2026-01-01 10:00:00'), -- Minor fault
('MED-1', 3, 0.4, 15.0, 'Active', '2026-01-01 10:00:00'),
('SAT-3', 1, 0.5, 10.0, 'Active', '2026-01-01 10:00:00'),
('SAT-3', 2, 0.5, 20.0, 'Active', '2026-01-01 10:00:00'),
('SAT-3', 3, 0.5, 30.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 8, 0.5, 35.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 9, 0.5, 40.0, 'Active', '2026-01-01 10:00:00'),
('ATL-1', 10, 0.5, 45.0, 'Active', '2026-01-01 10:00:00'),
('PAC-1', 6, 0.5, 48.0, 'Active', '2026-01-01 10:00:00'),
('PAC-1', 7, 0.5, 56.0, 'Active', '2026-01-01 10:00:00'),
('ATL-2', 6, 0.3, 24.0, 'Active', '2026-01-01 10:00:00'),
('ATL-2', 7, 0.3, 28.0, 'Active', '2026-01-01 10:00:00'),
('IND-1', 7, 15.0, 999.0, 'Broken', '2026-01-01 10:00:00'),
('PAC-2', 6, 0.6, 42.0, 'Active', '2026-01-01 10:00:00'),
('MED-1', 4, 0.4, 20.0, 'Active', '2026-01-01 10:00:00'),
('MED-1', 5, 0.4, 25.0, 'Active', '2026-01-01 10:00:00'),
('SAT-3', 4, 0.5, 40.0, 'Active', '2026-01-01 10:00:00'),
('SAT-3', 5, 0.5, 50.0, 'Active', '2026-01-01 10:00:00'),
('ARCTIC', 1, 1.0, 15.0, 'Active', '2026-01-01 10:00:00'), 
('ARCTIC', 2, 1.1, 30.0, 'Active', '2026-01-01 10:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Signal Health
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SubseaTelcoDB.Infrastructure.Silver.SegmentHealth AS
SELECT 
    CableID,
    SegmentID,
    SignalLossDB,
    LatencyMs,
    Status,
    ReadingTimestamp,
    -- Analysis
    CASE 
        WHEN SignalLossDB > 10.0 THEN 'Critical Failure'
        WHEN SignalLossDB > 3.0 THEN 'Warning'
        ELSE 'Healthy'
    END AS SignalCondition
FROM SubseaTelcoDB.Infrastructure.Bronze.CableReadings;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Maintenance Dispatch
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SubseaTelcoDB.Infrastructure.Gold.FaultLocations AS
SELECT 
    CableID,
    SignalCondition,
    COUNT(SegmentID) AS AffectedSegments,
    MIN(SegmentID) AS StartSegment,
    MAX(SegmentID) AS EndSegment,
    AVG(SignalLossDB) AS AvgLoss
FROM SubseaTelcoDB.Infrastructure.Silver.SegmentHealth
WHERE SignalCondition IN ('Critical Failure', 'Warning')
GROUP BY CableID, SignalCondition;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify the specific segments for 'ATL-1' that are in 'Warning' or 'Critical Failure' status."

PROMPT 2:
"List all cables with 'Broken' segments in the Bronze layer."

PROMPT 3:
"Calculate the max SignalLossDB recorded for 'IND-1' in the Silver layer."
*/
