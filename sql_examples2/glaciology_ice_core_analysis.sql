/*
 * Dremio Glaciology Ice Core Analysis Example
 * 
 * Domain: Climate Science & Geology
 * Scenario: 
 * Scientists drill ice cores from Antarctica and Greenland. Each core is sliced into "Strata" (layers)
 * representing different historical eras.
 * Lab analysis measures trapped gas concentrations (CO2, Methane) and Isotope ratios (Oxygen-18)
 * to reconstruct past temperatures.
 * 
 * Complexity: Medium (Depth-to-Age models, moving averages)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Climate_Lab;
CREATE FOLDER IF NOT EXISTS Climate_Lab.Sources;
CREATE FOLDER IF NOT EXISTS Climate_Lab.Bronze;
CREATE FOLDER IF NOT EXISTS Climate_Lab.Silver;
CREATE FOLDER IF NOT EXISTS Climate_Lab.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Climate_Lab.Sources.Ice_Core_Registry (
    CoreID VARCHAR,
    Drill_Site VARCHAR, -- 'Vostok', 'Dome-C'
    Extraction_Date DATE,
    Total_Depth_Meters DOUBLE,
    Principal_Investigator VARCHAR
);

CREATE TABLE IF NOT EXISTS Climate_Lab.Sources.Strata_Analysis (
    SampleID VARCHAR,
    CoreID VARCHAR,
    Depth_Top_Meters DOUBLE,
    Depth_Bottom_Meters DOUBLE,
    CO2_ppm DOUBLE,
    Methane_ppb DOUBLE,
    Oxygen18_Ratio DOUBLE, -- Proxy for temperature
    Dust_Particulates_Count INT, -- Volcanic activity marker
    Estimated_Age_YearsBP INT -- Years Before Present
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

INSERT INTO Climate_Lab.Sources.Ice_Core_Registry VALUES
('CORE-ANT-001', 'Vostok', '2020-01-15', 3000.0, 'Dr. P. Smith'),
('CORE-GRL-002', 'Greenland Summit', '2021-06-20', 2500.0, 'Dr. A. Jensen'),
('CORE-ANT-003', 'Dome-C', '2019-11-05', 3200.0, 'Dr. L. Wang');

-- Seed Analysis (Simulating data going back in time)
-- Deeper = Older
INSERT INTO Climate_Lab.Sources.Strata_Analysis VALUES
('SMP-001', 'CORE-ANT-001', 10.0, 11.0, 280.0, 700.0, -55.2, 50, 100), -- Recent (Pre-industrial)
('SMP-002', 'CORE-ANT-001', 50.0, 51.0, 275.0, 680.0, -55.5, 60, 500),
('SMP-003', 'CORE-ANT-001', 100.0, 101.0, 270.0, 650.0, -56.0, 45, 1000),
('SMP-004', 'CORE-ANT-001', 200.0, 201.0, 260.0, 600.0, -57.0, 30, 2500),
('SMP-005', 'CORE-ANT-001', 300.0, 301.0, 240.0, 550.0, -58.0, 25, 5000),
('SMP-006', 'CORE-ANT-001', 500.0, 501.0, 220.0, 450.0, -60.0, 80, 10000), -- Ice Age
('SMP-007', 'CORE-ANT-001', 600.0, 601.0, 200.0, 400.0, -62.0, 120, 12000),
('SMP-008', 'CORE-ANT-001', 700.0, 701.0, 190.0, 350.0, -63.0, 200, 15000), -- Glacial max
('SMP-009', 'CORE-ANT-001', 1000.0, 1001.0, 250.0, 600.0, -56.0, 50, 25000), -- Interglacial?
('SMP-010', 'CORE-ANT-001', 1500.0, 1501.0, 280.0, 700.0, -55.0, 40, 50000),
('SMP-011', 'CORE-GRL-002', 10.0, 11.0, 285.0, 720.0, -4.0, 100, 100),
('SMP-012', 'CORE-GRL-002', 50.0, 51.0, 280.0, 710.0, -4.2, 90, 500),
('SMP-013', 'CORE-GRL-002', 100.0, 101.0, 278.0, 690.0, -4.5, 80, 1000),
('SMP-014', 'CORE-GRL-002', 500.0, 501.0, 230.0, 500.0, -8.0, 200, 10000),
('SMP-015', 'CORE-GRL-002', 1000.0, 1001.0, 200.0, 400.0, -10.0, 300, 20000),
('SMP-016', 'CORE-ANT-003', 20.0, 21.0, 280.0, 700.0, -55.1, 55, 200),
('SMP-017', 'CORE-ANT-003', 60.0, 61.0, 275.0, 680.0, -55.4, 58, 600),
('SMP-018', 'CORE-ANT-003', 120.0, 121.0, 268.0, 640.0, -56.2, 48, 1200),
('SMP-019', 'CORE-ANT-003', 240.0, 241.0, 255.0, 590.0, -57.5, 35, 3000),
('SMP-020', 'CORE-ANT-003', 400.0, 401.0, 230.0, 520.0, -59.0, 30, 8000),
-- Recent samples with Volcanic Ash markers
('SMP-021', 'CORE-ANT-001', 5.0, 5.1, 290.0, 750.0, -55.0, 5000, 50), -- Eruption?
('SMP-022', 'CORE-GRL-002', 5.0, 5.1, 295.0, 760.0, -3.8, 4800, 50),
-- Deep Time
('SMP-023', 'CORE-ANT-001', 2000.0, 2001.0, 220.0, 450.0, -61.0, 90, 100000),
('SMP-024', 'CORE-ANT-001', 2500.0, 2501.0, 200.0, 400.0, -63.0, 100, 150000),
('SMP-025', 'CORE-ANT-001', 2900.0, 2901.0, 240.0, 550.0, -58.0, 60, 200000),
('SMP-026', 'CORE-ANT-003', 1500.0, 1501.0, 275.0, 680.0, -55.5, 45, 55000),
('SMP-027', 'CORE-ANT-003', 2000.0, 2001.0, 210.0, 430.0, -62.0, 110, 110000),
('SMP-028', 'CORE-ANT-003', 2500.0, 2501.0, 190.0, 380.0, -64.0, 150, 160000),
('SMP-029', 'CORE-ANT-003', 3000.0, 3001.0, 260.0, 620.0, -56.0, 50, 240000),
-- Filling more data...
('SMP-030', 'CORE-GRL-002', 150.0, 151.0, 270.0, 660.0, -5.0, 75, 1500),
('SMP-031', 'CORE-GRL-002', 200.0, 201.0, 265.0, 640.0, -5.5, 70, 2000),
('SMP-032', 'CORE-GRL-002', 300.0, 301.0, 250.0, 600.0, -6.5, 60, 4000),
('SMP-033', 'CORE-GRL-002', 400.0, 401.0, 240.0, 550.0, -7.0, 90, 6000),
('SMP-034', 'CORE-GRL-002', 600.0, 601.0, 220.0, 480.0, -8.5, 150, 12000),
('SMP-035', 'CORE-GRL-002', 700.0, 701.0, 210.0, 460.0, -9.0, 180, 14000),
('SMP-036', 'CORE-GRL-002', 800.0, 801.0, 205.0, 450.0, -9.2, 200, 16000),
('SMP-037', 'CORE-GRL-002', 900.0, 901.0, 202.0, 440.0, -9.5, 220, 18000),
('SMP-038', 'CORE-GRL-002', 1200.0, 1201.0, 215.0, 480.0, -8.8, 150, 25000),
('SMP-039', 'CORE-GRL-002', 1400.0, 1401.0, 230.0, 520.0, -7.5, 100, 30000),
('SMP-040', 'CORE-GRL-002', 1600.0, 1601.0, 250.0, 580.0, -6.0, 80, 40000),
('SMP-041', 'CORE-ANT-001', 55.0, 56.0, 278.0, 690.0, -55.3, 58, 550),
('SMP-042', 'CORE-ANT-001', 65.0, 66.0, 276.0, 685.0, -55.4, 57, 650),
('SMP-043', 'CORE-ANT-001', 75.0, 76.0, 274.0, 680.0, -55.6, 55, 750),
('SMP-044', 'CORE-ANT-001', 85.0, 86.0, 272.0, 675.0, -55.7, 53, 850),
('SMP-045', 'CORE-ANT-001', 95.0, 96.0, 271.0, 670.0, -55.8, 51, 950),
('SMP-046', 'CORE-ANT-003', 30.0, 31.0, 279.0, 695.0, -55.2, 54, 300),
('SMP-047', 'CORE-ANT-003', 40.0, 41.0, 278.0, 690.0, -55.3, 53, 400),
('SMP-048', 'CORE-ANT-003', 150.0, 151.0, 265.0, 630.0, -56.5, 45, 1500),
('SMP-049', 'CORE-ANT-003', 180.0, 181.0, 260.0, 620.0, -56.8, 42, 1800),
('SMP-050', 'CORE-ANT-003', 350.0, 351.0, 240.0, 550.0, -58.5, 35, 7000);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Climate_Lab.Bronze.Bronze_Cores AS SELECT * FROM Climate_Lab.Sources.Ice_Core_Registry;
CREATE OR REPLACE VIEW Climate_Lab.Bronze.Bronze_Strata AS SELECT * FROM Climate_Lab.Sources.Strata_Analysis;

-- 4b. SILVER LAYER (Climate Anomalies)
-- Calculate temperature deviation from a baseline (e.g., -55.0 for Antarctic)
CREATE OR REPLACE VIEW Climate_Lab.Silver.Silver_Climate_Records AS
SELECT
    s.SampleID,
    c.CoreID,
    c.Drill_Site,
    s.Estimated_Age_YearsBP,
    s.CO2_ppm,
    s.Methane_ppb,
    s.Oxygen18_Ratio,
    -- Simple anomaly proxy calc
    CASE 
        WHEN c.Drill_Site LIKE '%ANT%' THEN s.Oxygen18_Ratio - (-55.0)
        ELSE s.Oxygen18_Ratio - (-30.0) -- Greenland warmer baseline
    END as Temp_Anomaly_Proxy,
    s.Dust_Particulates_Count
FROM Climate_Lab.Bronze.Bronze_Strata s
JOIN Climate_Lab.Bronze.Bronze_Cores c ON s.CoreID = c.CoreID;

-- 4c. GOLD LAYER (Historic Trends)
CREATE OR REPLACE VIEW Climate_Lab.Gold.Gold_Atmospheric_Composition AS
SELECT
    CASE 
        WHEN Estimated_Age_YearsBP < 1000 THEN 'Recent (0-1ky)'
        WHEN Estimated_Age_YearsBP < 12000 THEN 'Holocene (1k-12ky)'
        WHEN Estimated_Age_YearsBP < 115000 THEN 'Glacial Period'
        ELSE 'Deep Past'
    END as Epoch,
    AVG(CO2_ppm) as Avg_CO2,
    AVG(Methane_ppb) as Avg_Methane,
    AVG(Temp_Anomaly_Proxy) as Avg_Temp_Anomaly,
    MAX(Dust_Particulates_Count) as Max_Dust_Event
FROM Climate_Lab.Silver.Silver_Climate_Records
GROUP BY Epoch
ORDER BY MIN(Estimated_Age_YearsBP);

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Correlate 'Avg_CO2' with 'Avg_Temp_Anomaly' in 'Gold_Atmospheric_Composition'. 
 * Is there a lagged relationship visible in the Glacial Period transitions?"
 */
