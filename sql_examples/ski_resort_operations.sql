/*
 * Dremio Ski Resort Operations Example
 * 
 * Domain: Hospitality & Logistics
 * Scenario: 
 * A major Ski Resort runs a complex operation balancing uphill lift capacity with 
 * downhill trail maintenance (grooming). 
 * Operations managers need to visualize "Lift Utilization" to reduce wait times 
 * and correlate it with "Grooming Logs" to ensure the most trafficked runs are 
 * smoothed out overnight.
 * 
 * Complexity: Medium (Time-series aggregations, utilization ratios)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Ski_Ops;
CREATE FOLDER IF NOT EXISTS Ski_Ops.Sources;
CREATE FOLDER IF NOT EXISTS Ski_Ops.Bronze;
CREATE FOLDER IF NOT EXISTS Ski_Ops.Silver;
CREATE FOLDER IF NOT EXISTS Ski_Ops.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Ski_Ops.Sources.Lift_Metadata (
    LiftID VARCHAR,
    Lift_Name VARCHAR,
    Capacity_Per_Hour INT,
    Base_Elevation_Ft INT,
    Top_Elevation_Ft INT
);

CREATE TABLE IF NOT EXISTS Ski_Ops.Sources.Lift_Scans (
    ScanID VARCHAR,
    PassID VARCHAR, -- Customer Pass
    LiftID VARCHAR,
    Scan_Timestamp TIMESTAMP,
    Access_Outcome VARCHAR -- 'Granted', 'Denied'
);

CREATE TABLE IF NOT EXISTS Ski_Ops.Sources.Groomer_Logs (
    LogID VARCHAR,
    GroomerID VARCHAR,
    Trail_Name VARCHAR,
    Groom_Start TIMESTAMP,
    Groom_End TIMESTAMP,
    Fuel_Used_Gal DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Lifts
INSERT INTO Ski_Ops.Sources.Lift_Metadata VALUES
('LFT-01', 'Summit Express', 2000, 8000, 12000),
('LFT-02', 'Bunny Hill', 500, 8000, 8100),
('LFT-03', 'Back Bowls', 1500, 10000, 13000),
('LFT-04', 'Park Quad', 1800, 8500, 10500),
('LFT-05', 'Gondola', 2500, 8000, 11000);

-- Seed Scans (Simulating a busy morning)
-- LFT-01 works hard
INSERT INTO Ski_Ops.Sources.Lift_Scans VALUES
('SCN-001', 'P-100', 'LFT-01', '2023-12-25 08:30:00', 'Granted'),
('SCN-002', 'P-101', 'LFT-01', '2023-12-25 08:30:05', 'Granted'),
('SCN-003', 'P-102', 'LFT-01', '2023-12-25 08:30:10', 'Granted'),
('SCN-004', 'P-103', 'LFT-01', '2023-12-25 08:30:15', 'Granted'),
('SCN-005', 'P-104', 'LFT-01', '2023-12-25 08:31:00', 'Granted'),
('SCN-006', 'P-105', 'LFT-01', '2023-12-25 08:31:05', 'Granted'),
('SCN-007', 'P-106', 'LFT-01', '2023-12-25 08:32:00', 'Granted'),
('SCN-008', 'P-107', 'LFT-01', '2023-12-25 08:32:05', 'Granted'),
('SCN-009', 'P-108', 'LFT-01', '2023-12-25 08:33:00', 'Granted'),
('SCN-010', 'P-109', 'LFT-01', '2023-12-25 08:33:10', 'Granted'),
('SCN-011', 'P-110', 'LFT-02', '2023-12-25 09:00:00', 'Granted'), -- Experts hitting bunny hill?
('SCN-012', 'P-111', 'LFT-02', '2023-12-25 09:05:00', 'Granted'),
('SCN-013', 'P-112', 'LFT-03', '2023-12-25 09:30:00', 'Granted'),
('SCN-014', 'P-113', 'LFT-03', '2023-12-25 09:30:30', 'Granted'),
('SCN-015', 'P-114', 'LFT-03', '2023-12-25 09:31:00', 'Granted'),
('SCN-016', 'P-115', 'LFT-05', '2023-12-25 08:00:00', 'Granted'),
('SCN-017', 'P-116', 'LFT-05', '2023-12-25 08:00:10', 'Granted'),
('SCN-018', 'P-117', 'LFT-05', '2023-12-25 08:00:20', 'Granted'),
('SCN-019', 'P-999', 'LFT-05', '2023-12-25 08:01:00', 'Denied'), -- Expired pass
('SCN-020', 'P-118', 'LFT-04', '2023-12-25 10:00:00', 'Granted'),
-- Bulk scans...
('SCN-021', 'P-200', 'LFT-01', '2023-12-25 09:00:00', 'Granted'),
('SCN-022', 'P-201', 'LFT-01', '2023-12-25 09:00:05', 'Granted'),
('SCN-023', 'P-202', 'LFT-01', '2023-12-25 09:00:10', 'Granted'),
('SCN-024', 'P-203', 'LFT-01', '2023-12-25 09:00:15', 'Granted'),
('SCN-025', 'P-204', 'LFT-01', '2023-12-25 09:01:00', 'Granted'),
('SCN-026', 'P-205', 'LFT-01', '2023-12-25 09:01:05', 'Granted'),
('SCN-027', 'P-206', 'LFT-01', '2023-12-25 09:02:00', 'Granted'),
('SCN-028', 'P-207', 'LFT-01', '2023-12-25 09:02:05', 'Granted'),
('SCN-029', 'P-208', 'LFT-01', '2023-12-25 09:03:00', 'Granted'),
('SCN-030', 'P-209', 'LFT-01', '2023-12-25 09:03:10', 'Granted'),
('SCN-031', 'P-210', 'LFT-03', '2023-12-25 10:00:00', 'Granted'),
('SCN-032', 'P-211', 'LFT-03', '2023-12-25 10:05:00', 'Granted'),
('SCN-033', 'P-212', 'LFT-03', '2023-12-25 10:30:00', 'Granted'),
('SCN-034', 'P-213', 'LFT-03', '2023-12-25 10:30:30', 'Granted'),
('SCN-035', 'P-214', 'LFT-03', '2023-12-25 10:31:00', 'Granted'),
('SCN-036', 'P-215', 'LFT-04', '2023-12-25 11:00:00', 'Granted'),
('SCN-037', 'P-216', 'LFT-04', '2023-12-25 11:00:10', 'Granted'),
('SCN-038', 'P-217', 'LFT-04', '2023-12-25 11:00:20', 'Granted'),
('SCN-039', 'P-888', 'LFT-04', '2023-12-25 11:01:00', 'Denied'),
('SCN-040', 'P-218', 'LFT-04', '2023-12-25 10:10:00', 'Granted'),
('SCN-041', 'P-300', 'LFT-02', '2023-12-25 09:10:00', 'Granted'),
('SCN-042', 'P-301', 'LFT-02', '2023-12-25 09:15:00', 'Granted'),
('SCN-043', 'P-302', 'LFT-02', '2023-12-25 09:20:00', 'Granted'),
('SCN-044', 'P-303', 'LFT-05', '2023-12-25 08:30:00', 'Granted'),
('SCN-045', 'P-304', 'LFT-05', '2023-12-25 08:31:00', 'Granted'),
('SCN-046', 'P-305', 'LFT-05', '2023-12-25 08:32:00', 'Granted'),
('SCN-047', 'P-306', 'LFT-05', '2023-12-25 08:33:00', 'Granted'),
('SCN-048', 'P-307', 'LFT-05', '2023-12-25 08:34:00', 'Granted'),
('SCN-049', 'P-308', 'LFT-05', '2023-12-25 08:35:00', 'Granted'),
('SCN-050', 'P-309', 'LFT-05', '2023-12-25 08:36:00', 'Granted');

-- Seed Grooming Logs (Overnight shift)
INSERT INTO Ski_Ops.Sources.Groomer_Logs VALUES
('G-100', 'CAT-1', 'Summit Run', '2023-12-24 22:00:00', '2023-12-24 23:30:00', 15.0),
('G-101', 'CAT-2', 'Bunny Hop', '2023-12-24 22:00:00', '2023-12-24 22:45:00', 8.0),
('G-102', 'CAT-1', 'The Plunge', '2023-12-24 23:45:00', '2023-12-25 01:00:00', 18.0), -- Steep run, more fuel
('G-103', 'CAT-3', 'Park Lane', '2023-12-24 22:00:00', '2023-12-25 02:00:00', 30.0),
('G-104', 'CAT-2', 'Easy Way', '2023-12-24 23:00:00', '2023-12-25 00:00:00', 10.0);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Ski_Ops.Bronze.Bronze_Scans AS SELECT * FROM Ski_Ops.Sources.Lift_Scans;
CREATE OR REPLACE VIEW Ski_Ops.Bronze.Bronze_Grooming AS SELECT * FROM Ski_Ops.Sources.Groomer_Logs;
CREATE OR REPLACE VIEW Ski_Ops.Bronze.Bronze_Metadata AS SELECT * FROM Ski_Ops.Sources.Lift_Metadata;

-- 4b. SILVER LAYER (Enriching Scans with Capacity Metadata)
CREATE OR REPLACE VIEW Ski_Ops.Silver.Silver_Lift_Traffic AS
SELECT
    s.ScanID,
    s.Scan_Timestamp,
    s.LiftID,
    m.Lift_Name,
    m.Capacity_Per_Hour,
    -- Simple hour truncation for traffic buckets
    TRUNC(s.Scan_Timestamp, 'HOUR') as Traffic_Hour
FROM Ski_Ops.Bronze.Bronze_Scans s
JOIN Ski_Ops.Bronze.Bronze_Metadata m ON s.LiftID = m.LiftID
WHERE s.Access_Outcome = 'Granted';

-- 4c. GOLD LAYER
CREATE OR REPLACE VIEW Ski_Ops.Gold.Gold_Lift_Utilization_Hourly AS
SELECT
    Lift_Name,
    Traffic_Hour,
    COUNT(*) as Riders_Count,
    MAX(Capacity_Per_Hour) as Capacity,
    -- Utilization %
    ROUND((COUNT(*) * 100.0 / NULLIF(MAX(Capacity_Per_Hour), 0)), 2) as Utilization_Percent
FROM Ski_Ops.Silver.Silver_Lift_Traffic
GROUP BY Lift_Name, Traffic_Hour
ORDER BY Utilization_Percent DESC;

CREATE OR REPLACE VIEW Ski_Ops.Gold.Gold_Grooming_Efficiency AS
SELECT
    GroomerID,
    COUNT(LogID) as Runs_Groomed,
    SUM(Fuel_Used_Gal) as Total_Fuel,
    SUM(EXTRACT(EPOCH FROM (Groom_End - Groom_Start))/3600.0) as Total_Hours,
    -- Fuel Burn Rate
    SUM(Fuel_Used_Gal) / NULLIF(SUM(EXTRACT(EPOCH FROM (Groom_End - Groom_Start))/3600.0), 0) as Gal_Per_Hour
FROM Ski_Ops.Bronze.Bronze_Grooming
GROUP BY GroomerID;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Compare 'Gold_Lift_Utilization_Hourly' stats for 'Summit Express' against 'Gold_Grooming_Efficiency'.
 * Does high utilization correlate with recent grooming on its primary trails?"
 */
