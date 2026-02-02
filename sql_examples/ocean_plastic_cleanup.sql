/*
 * Dremio Ocean Plastic Cleanup Logistics Example
 * 
 * Domain: Environmental Science & Logistics
 * Scenario: 
 * A non-profit is deploying autonomous solar-powered vessels to clean up plastic from the Great Pacific Garbage Patch.
 * Data from satellite density maps (static surveys) is combined with real-time telemetry from collectors (moving assets)
 * to calculate "Collection Efficiency". This helps reroute vessels to areas where they can have the highest impact.
 * 
 * Complexity: Medium-High (Route optimization logic, efficiency metrics)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Ocean_Cleanup;
CREATE FOLDER IF NOT EXISTS Ocean_Cleanup.Sources;
CREATE FOLDER IF NOT EXISTS Ocean_Cleanup.Bronze;
CREATE FOLDER IF NOT EXISTS Ocean_Cleanup.Silver;
CREATE FOLDER IF NOT EXISTS Ocean_Cleanup.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Ocean_Cleanup.Sources.Plastic_Density_Global (
    Grid_ID VARCHAR,
    Lat_Start DOUBLE,
    Lat_End DOUBLE,
    Lon_Start DOUBLE,
    Lon_End DOUBLE,
    Density_KG_M2 DOUBLE, -- KG of plastic per square meter
    Last_Survey_Date DATE,
    Confidence_Score DOUBLE -- Confidence in satellite reading
);

CREATE TABLE IF NOT EXISTS Ocean_Cleanup.Sources.Vessel_Telemetry (
    VesselID VARCHAR,
    Timestamp TIMESTAMP,
    Current_Lat DOUBLE,
    Current_Lon DOUBLE,
    Collection_Rate_KG_Hr DOUBLE, -- Rate of plastic collection
    Battery_Status INT,
    Storage_Capacity_Percent DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Plastic Density (Simulating Great Pacific Garbage Patch region)
INSERT INTO Ocean_Cleanup.Sources.Plastic_Density_Global VALUES
('GPGP-001', 30.0, 31.0, -140.0, -139.0, 5.2, '2023-01-01', 0.95),
('GPGP-002', 30.0, 31.0, -139.0, -138.0, 6.1, '2023-01-01', 0.92),
('GPGP-003', 30.0, 31.0, -138.0, -137.0, 4.8, '2023-01-01', 0.88),
('GPGP-004', 30.0, 31.0, -137.0, -136.0, 3.5, '2023-01-01', 0.90),
('GPGP-005', 30.0, 31.0, -136.0, -135.0, 2.1, '2023-01-01', 0.85),
('GPGP-006', 31.0, 32.0, -140.0, -139.0, 5.5, '2023-01-01', 0.94),
('GPGP-007', 31.0, 32.0, -139.0, -138.0, 7.2, '2023-01-01', 0.93),
('GPGP-008', 31.0, 32.0, -138.0, -137.0, 6.5, '2023-01-01', 0.91),
('GPGP-009', 31.0, 32.0, -137.0, -136.0, 4.2, '2023-01-01', 0.89),
('GPGP-010', 31.0, 32.0, -136.0, -135.0, 2.8, '2023-01-01', 0.86),
('GPGP-011', 32.0, 33.0, -140.0, -139.0, 4.9, '2023-01-01', 0.92),
('GPGP-012', 32.0, 33.0, -139.0, -138.0, 5.8, '2023-01-01', 0.90),
('GPGP-013', 32.0, 33.0, -138.0, -137.0, 5.1, '2023-01-01', 0.87),
('GPGP-014', 32.0, 33.0, -137.0, -136.0, 3.9, '2023-01-01', 0.85),
('GPGP-015', 32.0, 33.0, -136.0, -135.0, 1.5, '2023-01-01', 0.80),
('GPGP-016', 33.0, 34.0, -140.0, -139.0, 3.2, '2023-01-01', 0.91),
('GPGP-017', 33.0, 34.0, -139.0, -138.0, 4.1, '2023-01-01', 0.89),
('GPGP-018', 33.0, 34.0, -138.0, -137.0, 3.6, '2023-01-01', 0.86),
('GPGP-019', 33.0, 34.0, -137.0, -136.0, 2.4, '2023-01-01', 0.84),
('GPGP-020', 33.0, 34.0, -136.0, -135.0, 1.2, '2023-01-01', 0.82),
('GPGP-021', 29.0, 30.0, -140.0, -139.0, 4.5, '2023-01-01', 0.93),
('GPGP-022', 29.0, 30.0, -139.0, -138.0, 5.2, '2023-01-01', 0.91),
('GPGP-023', 29.0, 30.0, -138.0, -137.0, 4.1, '2023-01-01', 0.89),
('GPGP-024', 29.0, 30.0, -137.0, -136.0, 2.9, '2023-01-01', 0.86),
('GPGP-025', 29.0, 30.0, -136.0, -135.0, 1.8, '2023-01-01', 0.83),
('GPGP-026', 34.0, 35.0, -140.0, -139.0, 2.5, '2023-01-01', 0.90),
('GPGP-027', 34.0, 35.0, -139.0, -138.0, 3.1, '2023-01-01', 0.88),
('GPGP-028', 34.0, 35.0, -138.0, -137.0, 2.8, '2023-01-01', 0.85),
('GPGP-029', 34.0, 35.0, -137.0, -136.0, 1.9, '2023-01-01', 0.83),
('GPGP-030', 34.0, 35.0, -136.0, -135.0, 0.9, '2023-01-01', 0.81),
('ATL-001', 20.0, 21.0, -60.0, -59.0, 3.1, '2023-01-01', 0.88),
('ATL-002', 20.0, 21.0, -59.0, -58.0, 2.8, '2023-01-01', 0.86),
('ATL-003', 20.0, 21.0, -58.0, -57.0, 2.5, '2023-01-01', 0.85),
('ATL-004', 21.0, 22.0, -60.0, -59.0, 3.3, '2023-01-01', 0.89),
('ATL-005', 21.0, 22.0, -59.0, -58.0, 3.0, '2023-01-01', 0.87),
('ATL-006', 22.0, 23.0, -60.0, -59.0, 2.9, '2023-01-01', 0.88),
('IND-001', -10.0, -9.0, 70.0, 71.0, 4.2, '2023-01-01', 0.91),
('IND-002', -10.0, -9.0, 71.0, 72.0, 4.5, '2023-01-01', 0.92),
('IND-003', -10.0, -9.0, 72.0, 73.0, 4.0, '2023-01-01', 0.90),
('IND-004', -9.0, -8.0, 70.0, 71.0, 3.8, '2023-01-01', 0.89),
('IND-005', -9.0, -8.0, 71.0, 72.0, 4.1, '2023-01-01', 0.91),
('MED-001', 35.0, 36.0, 15.0, 16.0, 1.5, '2023-01-01', 0.85),
('MED-002', 35.0, 36.0, 16.0, 17.0, 1.8, '2023-01-01', 0.86),
('MED-003', 36.0, 37.0, 15.0, 16.0, 1.6, '2023-01-01', 0.85),
('MED-004', 36.0, 37.0, 16.0, 17.0, 1.9, '2023-01-01', 0.87),
('PCF-001', 10.0, 11.0, 130.0, 131.0, 2.2, '2023-01-01', 0.88),
('PCF-002', 10.0, 11.0, 131.0, 132.0, 2.4, '2023-01-01', 0.89),
('PCF-003', 11.0, 12.0, 130.0, 131.0, 2.1, '2023-01-01', 0.87),
('PCF-004', 11.0, 12.0, 131.0, 132.0, 2.3, '2023-01-01', 0.88),
('PCF-005', 12.0, 13.0, 130.0, 131.0, 2.0, '2023-01-01', 0.86),
('PCF-006', 12.0, 13.0, 131.0, 132.0, 2.1, '2023-01-01', 0.87);

-- Seed Vessel Telemetry
-- Simulating 5 vessels (Jenny, System 001, Interceptor 1, etc.)
INSERT INTO Ocean_Cleanup.Sources.Vessel_Telemetry VALUES
('VSL-001', '2023-06-01 12:00:00', 30.5, -139.5, 45.0, 90, 10.0),
('VSL-001', '2023-06-01 13:00:00', 30.55, -139.45, 50.0, 88, 12.0),
('VSL-001', '2023-06-01 14:00:00', 30.6, -139.4, 48.0, 86, 14.0),
('VSL-001', '2023-06-01 15:00:00', 30.65, -139.35, 52.0, 84, 16.0),
('VSL-001', '2023-06-01 16:00:00', 30.7, -139.3, 55.0, 82, 18.0),
('VSL-002', '2023-06-01 12:00:00', 31.5, -138.5, 60.0, 95, 5.0),
('VSL-002', '2023-06-01 13:00:00', 31.55, -138.45, 62.0, 93, 7.0),
('VSL-002', '2023-06-01 14:00:00', 31.6, -138.4, 58.0, 91, 9.0),
('VSL-002', '2023-06-01 15:00:00', 31.65, -138.35, 61.0, 89, 11.0),
('VSL-002', '2023-06-01 16:00:00', 31.7, -138.3, 63.0, 87, 13.0),
('VSL-003', '2023-06-01 12:00:00', 32.5, -137.5, 30.0, 80, 50.0),
('VSL-003', '2023-06-01 13:00:00', 32.55, -137.45, 25.0, 78, 52.0),
('VSL-003', '2023-06-01 14:00:00', 32.6, -137.4, 20.0, 75, 54.0), -- Slow collection
('VSL-003', '2023-06-01 15:00:00', 32.65, -137.35, 15.0, 72, 55.0),
('VSL-003', '2023-06-01 16:00:00', 32.7, -137.3, 10.0, 70, 56.0),
('VSL-004', '2023-06-01 12:00:00', 33.5, -136.5, 10.0, 99, 2.0),
('VSL-004', '2023-06-01 13:00:00', 33.55, -136.45, 12.0, 98, 3.0),
('VSL-004', '2023-06-01 14:00:00', 33.6, -136.4, 11.0, 97, 4.0),
('VSL-004', '2023-06-01 15:00:00', 33.65, -136.35, 13.0, 96, 5.0),
('VSL-004', '2023-06-01 16:00:00', 33.7, -136.3, 14.0, 95, 6.0),
('VSL-005', '2023-06-01 12:00:00', 29.5, -139.5, 40.0, 100, 0.0), -- Fresh deployment
('VSL-005', '2023-06-01 13:00:00', 29.55, -139.45, 42.0, 99, 1.0),
('VSL-005', '2023-06-01 14:00:00', 29.6, -139.4, 45.0, 98, 2.0),
('VSL-005', '2023-06-01 15:00:00', 29.65, -139.35, 47.0, 97, 3.0),
('VSL-005', '2023-06-01 16:00:00', 29.7, -139.3, 49.0, 96, 4.0),
('VSL-001', '2023-06-02 12:00:00', 30.52, -139.48, 46.0, 88, 10.5),
('VSL-001', '2023-06-02 13:00:00', 30.57, -139.43, 51.0, 86, 12.5),
('VSL-001', '2023-06-02 14:00:00', 30.62, -139.38, 49.0, 84, 14.5),
('VSL-001', '2023-06-02 15:00:00', 30.67, -139.33, 53.0, 82, 16.5),
('VSL-001', '2023-06-02 16:00:00', 30.72, -139.28, 56.0, 80, 18.5),
('VSL-002', '2023-06-02 12:00:00', 31.52, -138.48, 61.0, 93, 5.5),
('VSL-002', '2023-06-02 13:00:00', 31.57, -138.43, 63.0, 91, 7.5),
('VSL-002', '2023-06-02 14:00:00', 31.62, -138.38, 59.0, 89, 9.5),
('VSL-002', '2023-06-02 15:00:00', 31.67, -138.33, 62.0, 87, 11.5),
('VSL-002', '2023-06-02 16:00:00', 31.72, -138.28, 64.0, 85, 13.5),
('VSL-003', '2023-06-02 12:00:00', 32.52, -137.48, 31.0, 78, 50.5),
('VSL-003', '2023-06-02 13:00:00', 32.57, -137.43, 26.0, 76, 52.5),
('VSL-003', '2023-06-02 14:00:00', 32.62, -137.38, 21.0, 73, 54.5),
('VSL-003', '2023-06-02 15:00:00', 32.67, -137.33, 16.0, 70, 55.5),
('VSL-003', '2023-06-02 16:00:00', 32.72, -137.28, 11.0, 68, 56.5),
('VSL-004', '2023-06-02 12:00:00', 33.52, -136.48, 11.0, 97, 2.5),
('VSL-004', '2023-06-02 13:00:00', 33.57, -136.43, 13.0, 96, 3.5),
('VSL-004', '2023-06-02 14:00:00', 33.62, -136.38, 12.0, 95, 4.5),
('VSL-004', '2023-06-02 15:00:00', 33.67, -136.33, 14.0, 94, 5.5),
('VSL-004', '2023-06-02 16:00:00', 33.72, -136.28, 15.0, 93, 6.5),
('VSL-005', '2023-06-02 12:00:00', 29.52, -139.48, 41.0, 98, 0.5),
('VSL-005', '2023-06-02 13:00:00', 29.57, -139.43, 43.0, 97, 1.5),
('VSL-005', '2023-06-02 14:00:00', 29.62, -139.38, 46.0, 96, 2.5),
('VSL-005', '2023-06-02 15:00:00', 29.67, -139.33, 48.0, 95, 3.5),
('VSL-005', '2023-06-02 16:00:00', 29.72, -139.28, 50.0, 94, 4.5);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER (Raw Telemetry)
CREATE OR REPLACE VIEW Ocean_Cleanup.Bronze.Bronze_Vessel_Telemetry AS
SELECT
    VesselID,
    Timestamp,
    Current_Lat,
    Current_Lon,
    Collection_Rate_KG_Hr,
    Battery_Status,
    Storage_Capacity_Percent
FROM Ocean_Cleanup.Sources.Vessel_Telemetry;

CREATE OR REPLACE VIEW Ocean_Cleanup.Bronze.Bronze_Plastic_Density AS
SELECT
    Grid_ID,
    Lat_Start,
    Lat_End,
    Lon_Start,
    Lon_End,
    Density_KG_M2,
    Last_Survey_Date,
    Confidence_Score
FROM Ocean_Cleanup.Sources.Plastic_Density_Global;

-- 4b. SILVER LAYER (Spatial Join - Matching Vessel to Grid map to measure context)
CREATE OR REPLACE VIEW Ocean_Cleanup.Silver.Silver_Vessel_Performance AS
SELECT
    v.VesselID,
    v.Timestamp,
    v.Collection_Rate_KG_Hr,
    v.Storage_Capacity_Percent,
    d.Grid_ID,
    d.Density_KG_M2,
    -- Efficiency = Collection Rate / Available Density (metric of header effectiveness)
    v.Collection_Rate_KG_Hr / NULLIF(d.Density_KG_M2, 0) as Header_Efficiency_Index
FROM Ocean_Cleanup.Bronze.Bronze_Vessel_Telemetry v
JOIN Ocean_Cleanup.Bronze.Bronze_Plastic_Density d
  ON v.Current_Lat BETWEEN d.Lat_Start AND d.Lat_End
  AND v.Current_Lon BETWEEN d.Lon_Start AND d.Lon_End;

-- 4c. GOLD LAYER (Business Aggregates)
CREATE OR REPLACE VIEW Ocean_Cleanup.Gold.Gold_Fleet_Route_Optimization AS
SELECT
    Grid_ID,
    AVG(Density_KG_M2) as Avg_Density,
    COUNT(DISTINCT VesselID) as Vessel_Visits,
    AVG(Header_Efficiency_Index) as Mean_Efficiency
FROM Ocean_Cleanup.Silver.Silver_Vessel_Performance
GROUP BY Grid_ID
ORDER BY Avg_Density DESC;

CREATE OR REPLACE VIEW Ocean_Cleanup.Gold.Gold_Total_Impact_Report AS
SELECT
    VesselID,
    TRUNC(Timestamp, 'DAY') as Report_Date,
    SUM(Collection_Rate_KG_Hr) as Total_KG_Collected_Est,
    MAX(Storage_Capacity_Percent) as Peak_Storage_Risk
FROM Ocean_Cleanup.Silver.Silver_Vessel_Performance
GROUP BY VesselID, TRUNC(Timestamp, 'DAY');

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Examine 'Gold_Fleet_Route_Optimization' to find high-density grids (Density > 5.0) that have zero vessel visits.
 * Suggest moving vessels from low-efficiency grids to these targets."
 */
