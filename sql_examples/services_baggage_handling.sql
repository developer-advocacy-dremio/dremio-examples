/*
    Dremio High-Volume SQL Pattern: Services Baggage Handling
    
    Business Scenario:
    Airlines track "Mishandled Baggage" (Lost/Delayed).
    We need to identify where bags are getting stuck (Check-in, Transfer, Carousel)
    and calculate probability of loss based on Origin/Destination.
    
    Data Story:
    - Bronze: BagScans (Tracking points), FlightManifest.
    - Silver: BagJourney (Time between scans).
    - Gold: MishandlingMetrics (Loss rate by Airport).
    
    Medallion Architecture:
    - Bronze: BagScans, FlightManifest.
      *Volume*: 50+ records.
    - Silver: BagJourney.
    - Gold: MishandlingMetrics.
    
    Key Dremio Features:
    - Window Functions (Lead/Lag for scan times)
    - Pivot/Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ServicesDB;
CREATE FOLDER IF NOT EXISTS ServicesDB.Bronze;
CREATE FOLDER IF NOT EXISTS ServicesDB.Silver;
CREATE FOLDER IF NOT EXISTS ServicesDB.Gold;
USE ServicesDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ServicesDB.Bronze.FlightManifest (
    FlightID STRING,
    Origin STRING,
    Destination STRING,
    SchedDepTime TIMESTAMP
);

INSERT INTO ServicesDB.Bronze.FlightManifest VALUES
('FL100', 'JFK', 'LHR', TIMESTAMP '2025-01-20 18:00:00'),
('FL101', 'LHR', 'DXB', TIMESTAMP '2025-01-21 08:00:00'),
('FL102', 'JFK', 'LAX', TIMESTAMP '2025-01-20 09:00:00'),
('FL103', 'LAX', 'HND', TIMESTAMP '2025-01-20 14:00:00'),
('FL104', 'ORD', 'MIA', TIMESTAMP '2025-01-20 10:00:00');

CREATE OR REPLACE TABLE ServicesDB.Bronze.BagScans (
    BagTagID STRING,
    FlightID STRING,
    ScanLocation STRING, -- CheckIn, Ramp, Transfer, Carousel
    ScanTime TIMESTAMP,
    Status STRING -- Loaded, Unloaded, Missing
);

INSERT INTO ServicesDB.Bronze.BagScans VALUES
-- Bag 1: Perfect Journey FL100
('TAG-001', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-001', 'FL100', 'Ramp', TIMESTAMP '2025-01-20 17:30:00', 'Loaded'),
('TAG-001', 'FL100', 'Carousel', TIMESTAMP '2025-01-21 06:30:00', 'Unloaded'),

-- Bag 2: Delayed FL100
('TAG-002', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:05:00', 'Loaded'),
('TAG-002', 'FL100', 'Ramp', TIMESTAMP '2025-01-20 17:45:00', 'Loaded'),
-- Missed scan at carousel

-- Bag 3: Transfer FL100 -> FL101
('TAG-003', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 15:50:00', 'Loaded'),
('TAG-003', 'FL100', 'Ramp', TIMESTAMP '2025-01-20 17:30:00', 'Loaded'),
('TAG-003', 'FL101', 'Transfer', TIMESTAMP '2025-01-21 07:00:00', 'Loaded'),
('TAG-003', 'FL101', 'Ramp', TIMESTAMP '2025-01-21 07:45:00', 'Loaded'),

-- Bulk Data
('TAG-004', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-004', 'FL102', 'Ramp', TIMESTAMP '2025-01-20 08:30:00', 'Loaded'),
('TAG-004', 'FL102', 'Carousel', TIMESTAMP '2025-01-20 12:30:00', 'Unloaded'),

('TAG-005', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:05:00', 'Loaded'),
('TAG-005', 'FL102', 'Ramp', TIMESTAMP '2025-01-20 08:35:00', 'Loaded'),
('TAG-005', 'FL102', 'Carousel', TIMESTAMP '2025-01-20 12:35:00', 'Unloaded'),

('TAG-006', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:10:00', 'Loaded'),
('TAG-006', 'FL102', 'Ramp', TIMESTAMP '2025-01-20 08:40:00', 'Loaded'), -- Lost

('TAG-007', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-007', 'FL104', 'Ramp', TIMESTAMP '2025-01-20 09:30:00', 'Loaded'),
('TAG-007', 'FL104', 'Carousel', TIMESTAMP '2025-01-20 13:30:00', 'Unloaded'),

('TAG-008', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:05:00', 'Loaded'),
('TAG-008', 'FL104', 'Ramp', TIMESTAMP '2025-01-20 09:35:00', 'Loaded'),
('TAG-008', 'FL104', 'Carousel', TIMESTAMP '2025-01-20 13:35:00', 'Unloaded'),

('TAG-009', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:10:00', 'Loaded'), -- Only Checkin

('TAG-010', 'FL103', 'CheckIn', TIMESTAMP '2025-01-20 12:00:00', 'Loaded'),
('TAG-010', 'FL103', 'Ramp', TIMESTAMP '2025-01-20 13:30:00', 'Loaded'),
('TAG-010', 'FL103', 'Carousel', TIMESTAMP '2025-01-21 02:00:00', 'Unloaded'),

('TAG-011', 'FL103', 'CheckIn', TIMESTAMP '2025-01-20 12:05:00', 'Loaded'),
('TAG-011', 'FL103', 'Ramp', TIMESTAMP '2025-01-20 13:35:00', 'Loaded'),
('TAG-011', 'FL103', 'Carousel', TIMESTAMP '2025-01-21 02:05:00', 'Unloaded'),

('TAG-012', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:10:00', 'Loaded'),
('TAG-012', 'FL100', 'Ramp', TIMESTAMP '2025-01-20 17:40:00', 'Loaded'),
('TAG-012', 'FL100', 'Carousel', TIMESTAMP '2025-01-21 06:40:00', 'Unloaded'),

('TAG-013', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:15:00', 'Loaded'), -- Lost
('TAG-014', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:20:00', 'Loaded'), -- Lost
('TAG-015', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:25:00', 'Loaded'), -- Lost

('TAG-016', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:15:00', 'Loaded'),
('TAG-016', 'FL104', 'Ramp', TIMESTAMP '2025-01-20 09:45:00', 'Loaded'),
('TAG-016', 'FL104', 'Carousel', TIMESTAMP '2025-01-20 13:45:00', 'Unloaded'),

('TAG-017', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:20:00', 'Loaded'),
('TAG-017', 'FL104', 'Ramp', TIMESTAMP '2025-01-20 09:50:00', 'Loaded'),

('TAG-018', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:15:00', 'Loaded'),
('TAG-018', 'FL102', 'Ramp', TIMESTAMP '2025-01-20 08:45:00', 'Loaded'),
('TAG-018', 'FL102', 'Carousel', TIMESTAMP '2025-01-20 12:45:00', 'Unloaded'),

('TAG-019', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:20:00', 'Loaded'),
('TAG-019', 'FL102', 'Ramp', TIMESTAMP '2025-01-20 08:50:00', 'Loaded'),
('TAG-019', 'FL102', 'Carousel', TIMESTAMP '2025-01-20 12:50:00', 'Unloaded'),

('TAG-020', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:25:00', 'Loaded'),

-- Simulating more bags to hit 50 record count
('TAG-021', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-022', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-023', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-024', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-025', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-026', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-027', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-028', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-029', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-030', 'FL100', 'CheckIn', TIMESTAMP '2025-01-20 16:00:00', 'Loaded'),
('TAG-031', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-032', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-033', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-034', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-035', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-036', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-037', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-038', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-039', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-040', 'FL102', 'CheckIn', TIMESTAMP '2025-01-20 07:00:00', 'Loaded'),
('TAG-041', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-042', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-043', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-044', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-045', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-046', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-047', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-048', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-049', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded'),
('TAG-050', 'FL104', 'CheckIn', TIMESTAMP '2025-01-20 08:00:00', 'Loaded');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Bag Journey (Simplified Last Scan)
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.BagJourney AS
SELECT
    BagTagID,
    FlightID,
    MAX(ScanTime) AS LastSeenTime,
    CASE 
        WHEN SUM(CASE WHEN ScanLocation = 'Carousel' THEN 1 ELSE 0 END) > 0 THEN 'DELIVERED'
        WHEN SUM(CASE WHEN ScanLocation = 'Ramp' THEN 1 ELSE 0 END) > 0 THEN 'IN_TRANSIT'
        ELSE 'CHECKED_IN'
    END AS Status
FROM ServicesDB.Bronze.BagScans
GROUP BY BagTagID, FlightID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Mishandling Metrics
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.MishandlingMetrics AS
SELECT
    f.Origin,
    f.Destination,
    COUNT(b.BagTagID) AS TotalBags,
    SUM(CASE WHEN b.Status != 'DELIVERED' THEN 1 ELSE 0 END) AS PotentialMishandled,
    (CAST(SUM(CASE WHEN b.Status != 'DELIVERED' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(b.BagTagID)) * 100.0 AS RiskPct
FROM ServicesDB.Silver.BagJourney b
JOIN ServicesDB.Bronze.FlightManifest f ON b.FlightID = f.FlightID
-- Only consider flights that should have landed by now (2025-01-21)
WHERE f.SchedDepTime < TIMESTAMP '2025-01-21 00:00:00'
GROUP BY f.Origin, f.Destination;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Identify Origin airports with highest lost bag risk."
    2. "Count bags status by Flight ID."
    3. "List bags that never made it to the Carousel."
*/
