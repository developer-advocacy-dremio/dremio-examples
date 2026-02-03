/*
 * Dremio Railway Track Geometry Example
 * 
 * Domain: Transportation Engineering (Rail)
 * Scenario: 
 * Modern trains run "Geometry Cars" that inspect the track at high speed.
 * They measure "Gauge" (width between rails), "Cant" (super-elevation on curves), 
 * "Twist" (change in cross-level), and "Alignment".
 * The goal is to identify defects that could cause derailments (e.g., Wide Gauge) 
 * and issue "Slow Orders" to drivers until maintenance fixes match.
 * 
 * Complexity: Medium (Linear referencing, threshold classification)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Rail_Maintenance;
CREATE FOLDER IF NOT EXISTS Rail_Maintenance.Sources;
CREATE FOLDER IF NOT EXISTS Rail_Maintenance.Bronze;
CREATE FOLDER IF NOT EXISTS Rail_Maintenance.Bronze;
CREATE FOLDER IF NOT EXISTS Rail_Maintenance.Silver;
CREATE FOLDER IF NOT EXISTS Rail_Maintenance.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Rail_Maintenance.Sources.Track_Assets (
    TrackID VARCHAR,
    Line_Name VARCHAR, -- 'Northeast Corridor', 'Pacific Main'
    Segment_Start_MP DOUBLE, -- Milepost
    Segment_End_MP DOUBLE,
    Speed_Limit_MPH INT,
    Track_Class INT -- 1 (Slow) to 5 (High Speed)
);

CREATE TABLE IF NOT EXISTS Rail_Maintenance.Sources.Geometry_Car_Runs (
    RunID VARCHAR,
    TrackID VARCHAR,
    Milepost DOUBLE,
    Run_Date DATE,
    Gauge_Inch DOUBLE, -- Standard is 56.5
    Cross_Level_Inch DOUBLE, -- Elevation diff between rails
    Alignment_Inch DOUBLE, -- Standard deviation from straight
    Car_ID VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Track Assets
INSERT INTO Rail_Maintenance.Sources.Track_Assets VALUES
('TRK-001', 'Northeast Corridor', 0.0, 100.0, 125, 5),
('TRK-002', 'Empire Line', 0.0, 50.0, 80, 4),
('TRK-003', 'Freight Loop', 200.0, 250.0, 40, 2);

-- Seed Geometry Data (Simulating a run down the line)
-- Standard Gauge: 56.5. Wide gauge > 57.5 is bad.
INSERT INTO Rail_Maintenance.Sources.Geometry_Car_Runs VALUES
('RUN-100', 'TRK-001', 0.1, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 0.2, '2023-11-01', 56.5, 0.0, 0.02, 'GEO-1'),
('RUN-100', 'TRK-001', 0.3, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 0.4, '2023-11-01', 56.6, 0.1, 0.03, 'GEO-1'),
('RUN-100', 'TRK-001', 0.5, '2023-11-01', 56.7, 0.1, 0.04, 'GEO-1'),
('RUN-100', 'TRK-001', 0.6, '2023-11-01', 56.8, 0.2, 0.05, 'GEO-1'), -- Gauge widening
('RUN-100', 'TRK-001', 0.7, '2023-11-01', 57.0, 0.2, 0.06, 'GEO-1'),
('RUN-100', 'TRK-001', 0.8, '2023-11-01', 57.2, 0.3, 0.07, 'GEO-1'),
('RUN-100', 'TRK-001', 0.9, '2023-11-01', 57.5, 0.3, 0.08, 'GEO-1'), -- Alert Level 1
('RUN-100', 'TRK-001', 1.0, '2023-11-01', 57.8, 0.4, 0.10, 'GEO-1'), -- Critical! Wide Gauge
('RUN-100', 'TRK-001', 1.1, '2023-11-01', 57.6, 0.3, 0.09, 'GEO-1'),
('RUN-100', 'TRK-001', 1.2, '2023-11-01', 57.0, 0.2, 0.05, 'GEO-1'), -- Recovering
('RUN-100', 'TRK-001', 1.3, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 50.0, '2023-11-01', 56.5, 1.0, 0.01, 'GEO-1'), -- Curve Start
('RUN-100', 'TRK-001', 50.1, '2023-11-01', 56.5, 2.0, 0.02, 'GEO-1'),
('RUN-100', 'TRK-001', 50.2, '2023-11-01', 56.5, 3.0, 0.03, 'GEO-1'),
('RUN-100', 'TRK-001', 50.3, '2023-11-01', 56.5, 4.0, 0.04, 'GEO-1'), -- High Cant (Superelevation)
('RUN-100', 'TRK-001', 50.4, '2023-11-01', 56.5, 4.0, 0.04, 'GEO-1'),
('RUN-100', 'TRK-001', 50.5, '2023-11-01', 56.5, 4.0, 0.04, 'GEO-1'),
('RUN-100', 'TRK-001', 50.6, '2023-11-01', 56.5, 4.0, 0.04, 'GEO-1'),
('RUN-100', 'TRK-001', 50.7, '2023-11-01', 56.5, 3.0, 0.03, 'GEO-1'), -- Curve End
('RUN-100', 'TRK-001', 50.8, '2023-11-01', 56.5, 1.0, 0.02, 'GEO-1'), -- Twist defect possible here?
('RUN-100', 'TRK-001', 50.9, '2023-11-01', 56.5, -1.0, 0.02, 'GEO-1'), -- Rapid change! 1.0 to -1.0
('RUN-100', 'TRK-001', 51.0, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-101', 'TRK-002', 10.0, '2023-11-02', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-101', 'TRK-002', 10.1, '2023-11-02', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-101', 'TRK-002', 10.2, '2023-11-02', 56.2, 0.0, 0.01, 'GEO-1'), -- Tight gauge
('RUN-101', 'TRK-002', 10.3, '2023-11-02', 56.0, 0.0, 0.01, 'GEO-1'), -- Standard min is 56.0
('RUN-101', 'TRK-002', 10.4, '2023-11-02', 55.8, 0.0, 0.01, 'GEO-1'), -- Too tight!
('RUN-101', 'TRK-002', 10.5, '2023-11-02', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-102', 'TRK-003', 200.1, '2023-11-03', 56.5, 0.0, 0.50, 'GEO-2'), -- Basic Freight alignment issues
('RUN-102', 'TRK-003', 200.2, '2023-11-03', 56.5, 0.0, 0.60, 'GEO-2'),
('RUN-102', 'TRK-003', 200.3, '2023-11-03', 56.5, 0.0, 0.70, 'GEO-2'),
('RUN-102', 'TRK-003', 200.4, '2023-11-03', 56.5, 0.0, 0.80, 'GEO-2'), -- Rough ride
('RUN-102', 'TRK-003', 200.5, '2023-11-03', 56.5, 0.0, 0.90, 'GEO-2'),
('RUN-102', 'TRK-003', 200.6, '2023-11-03', 56.5, 0.0, 1.00, 'GEO-2'),
('RUN-102', 'TRK-003', 200.7, '2023-11-03', 56.5, 0.0, 0.60, 'GEO-2'),
('RUN-100', 'TRK-001', 90.0, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.1, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.2, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.3, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.4, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.5, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.6, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.7, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.8, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 90.9, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 91.0, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 91.1, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 91.2, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1'),
('RUN-100', 'TRK-001', 91.3, '2023-11-01', 56.5, 0.0, 0.01, 'GEO-1');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Rail_Maintenance.Bronze.Bronze_Geometry AS SELECT * FROM Rail_Maintenance.Sources.Geometry_Car_Runs;
CREATE OR REPLACE VIEW Rail_Maintenance.Bronze.Bronze_Assets AS SELECT * FROM Rail_Maintenance.Sources.Track_Assets;

-- 4b. SILVER LAYER (Defect Classification)
CREATE OR REPLACE VIEW Rail_Maintenance.Silver.Silver_Track_Defects AS
SELECT
    g.RunID,
    g.TrackID,
    g.Milepost,
    g.Run_Date,
    a.Speed_Limit_MPH,
    a.Track_Class,
    g.Gauge_Inch,
    g.Cross_Level_Inch,
    -- Federal Safety Standards (Simulated logic per Track Class)
    -- Class 5 (High speed) has tighter tolerances
    CASE 
        WHEN a.Track_Class = 5 AND g.Gauge_Inch > 57.25 THEN 'Critical - Wide Gauge'
        WHEN a.Track_Class = 5 AND g.Gauge_Inch < 56.0 THEN 'Critical - Tight Gauge'
        WHEN a.Track_Class < 5 AND g.Gauge_Inch > 57.75 THEN 'Maintenance Required'
        ELSE 'Normal'
    END as Defect_Status
FROM Rail_Maintenance.Bronze.Bronze_Geometry g
JOIN Rail_Maintenance.Bronze.Bronze_Assets a ON g.TrackID = a.TrackID
  AND g.Milepost BETWEEN a.Segment_Start_MP AND a.Segment_End_MP;

-- 4c. GOLD LAYER (Slow Order Recommendations)
CREATE OR REPLACE VIEW Rail_Maintenance.Gold.Gold_Slow_Orders AS
SELECT
    TrackID,
    MIN(Milepost) as Start_MP,
    MAX(Milepost) as End_MP,
    COUNT(*) as Defect_Count,
    MAX(Defect_Status) as Severity,
    -- Recommendation
    CASE 
        WHEN MAX(Defect_Status) LIKE 'Critical%' THEN 10 -- Reduce speed to 10 MPH
        WHEN MAX(Defect_Status) LIKE 'Maintenance%' THEN 30 -- Reduce speed to 30 MPH
        ELSE NULL -- No restriction
    END as Recommended_Speed_Limit_MPH
FROM Rail_Maintenance.Silver.Silver_Track_Defects
WHERE Defect_Status != 'Normal'
GROUP BY TrackID, TRUNC(Milepost) -- Group by Mile block
ORDER BY TrackID, Start_MP;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Examine 'Gold_Slow_Orders' for defects on TrackID 'TRK-001'. 
 * If 'Wide Gauge' defects persist over sequential miles, flag for immediate tie replacement."
 */
