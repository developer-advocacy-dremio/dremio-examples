/*
 * Dremio Exoplanet Transit Hunt Example
 * 
 * Domain: Astrophysics & Space Science
 * Scenario: 
 * Space telescopes (like Kepler/TESS) stare at stars measuring "Flux" (brightness).
 * If a planet passes in front (Transit), the flux drops periodically.
 * The system analyzes "Light Curves" to find these dips and calculate "Orbital Period".
 * 
 * Complexity: Medium (Time series dip detection, period folding)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Astro_Physics_Lab;
CREATE FOLDER IF NOT EXISTS Astro_Physics_Lab.Sources;
CREATE FOLDER IF NOT EXISTS Astro_Physics_Lab.Bronze;
CREATE FOLDER IF NOT EXISTS Astro_Physics_Lab.Silver;
CREATE FOLDER IF NOT EXISTS Astro_Physics_Lab.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Astro_Physics_Lab.Sources.Target_Stars (
    KeplerID VARCHAR,
    Star_Type VARCHAR, -- 'G-Type', 'M-Dwarf', 'Red Giant'
    Teph_Kelvin INT, -- Effective Temperature
    Radius_Solar INT -- Multiples of Sun radius
);

CREATE TABLE IF NOT EXISTS Astro_Physics_Lab.Sources.Light_Curves (
    CurveID VARCHAR,
    KeplerID VARCHAR,
    Observation_Time TIMESTAMP,
    Flux_Electron_Count DOUBLE, -- Brightness
    Quality_Flag INT -- 0=Good, 1=Cosmic Ray
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Stars
INSERT INTO Astro_Physics_Lab.Sources.Target_Stars VALUES
('KIC-101', 'G-Type', 5800, 1), -- Sun-like
('KIC-102', 'M-Dwarf', 3200, 0.5), -- Small, dim
('KIC-103', 'Red Giant', 4500, 10);

-- Seed Light Curves (Simulating a transit on KIC-101 every 4 units)
-- Baseline Flux = 1000. Transit drops to 990.
INSERT INTO Astro_Physics_Lab.Sources.Light_Curves VALUES
('LC-001', 'KIC-101', '2023-01-01 00:00:00', 1000.0, 0),
('LC-002', 'KIC-101', '2023-01-01 01:00:00', 1001.0, 0),
('LC-003', 'KIC-101', '2023-01-01 02:00:00', 999.0, 0),
('LC-004', 'KIC-101', '2023-01-01 03:00:00', 990.0, 0), -- Transit Start
('LC-005', 'KIC-101', '2023-01-01 04:00:00', 985.0, 0), -- Deepest
('LC-006', 'KIC-101', '2023-01-01 05:00:00', 990.0, 0), -- Transit End
('LC-007', 'KIC-101', '2023-01-01 06:00:00', 1000.0, 0),
('LC-008', 'KIC-101', '2023-01-01 07:00:00', 1002.0, 0),
('LC-009', 'KIC-101', '2023-01-01 08:00:00', 999.0, 0),
('LC-010', 'KIC-101', '2023-01-01 09:00:00', 1000.0, 0),
('LC-011', 'KIC-101', '2023-01-01 10:00:00', 1000.0, 0),
('LC-012', 'KIC-101', '2023-01-01 11:00:00', 1001.0, 0),
('LC-013', 'KIC-101', '2023-01-01 12:00:00', 990.0, 0), -- Second Transit (Period ~9h? Simulation speedup)
('LC-014', 'KIC-101', '2023-01-01 13:00:00', 985.0, 0),
('LC-015', 'KIC-101', '2023-01-01 14:00:00', 990.0, 0),
('LC-016', 'KIC-101', '2023-01-01 15:00:00', 1000.0, 0),
('LC-017', 'KIC-102', '2023-01-01 00:00:00', 500.0, 0), -- Dim star
('LC-018', 'KIC-102', '2023-01-01 01:00:00', 501.0, 0),
('LC-019', 'KIC-102', '2023-01-01 02:00:00', 499.0, 0),
('LC-020', 'KIC-102', '2023-01-01 03:00:00', 500.0, 0),
('LC-021', 'KIC-102', '2023-01-01 04:00:00', 500.0, 0), -- No transit
('LC-022', 'KIC-102', '2023-01-01 05:00:00', 500.0, 0),
('LC-023', 'KIC-103', '2023-01-01 00:00:00', 5000.0, 0), -- Bright giant
('LC-024', 'KIC-103', '2023-01-01 01:00:00', 5050.0, 0), -- Noisy
('LC-025', 'KIC-103', '2023-01-01 02:00:00', 4950.0, 0),
-- More KIC-101 data to secure period
('LC-026', 'KIC-101', '2023-01-01 16:00:00', 1000.0, 0),
('LC-027', 'KIC-101', '2023-01-01 17:00:00', 1000.0, 0),
('LC-028', 'KIC-101', '2023-01-01 18:00:00', 1000.0, 0),
('LC-029', 'KIC-101', '2023-01-01 19:00:00', 1000.0, 0),
('LC-030', 'KIC-101', '2023-01-01 20:00:00', 1000.0, 0),
('LC-031', 'KIC-101', '2023-01-01 21:00:00', 990.0, 0), -- Third Transit
('LC-032', 'KIC-101', '2023-01-01 22:00:00', 985.0, 0),
('LC-033', 'KIC-101', '2023-01-01 23:00:00', 990.0, 0),
('LC-034', 'KIC-101', '2023-01-02 00:00:00', 1000.0, 0),
('LC-035', 'KIC-101', '2023-01-02 01:00:00', 1000.0, 0),
('LC-036', 'KIC-102', '2023-01-01 06:00:00', 500.0, 0),
('LC-037', 'KIC-102', '2023-01-01 07:00:00', 500.0, 0),
('LC-038', 'KIC-102', '2023-01-01 08:00:00', 500.0, 0),
('LC-039', 'KIC-102', '2023-01-01 09:00:00', 500.0, 0),
('LC-040', 'KIC-102', '2023-01-01 10:00:00', 500.0, 0),
('LC-041', 'KIC-102', '2023-01-01 11:00:00', 500.0, 0),
('LC-042', 'KIC-102', '2023-01-01 12:00:00', 500.0, 0),
('LC-043', 'KIC-102', '2023-01-01 13:00:00', 500.0, 0),
('LC-044', 'KIC-102', '2023-01-01 14:00:00', 500.0, 0),
('LC-045', 'KIC-103', '2023-01-01 03:00:00', 5000.0, 1), -- Bad data
('LC-046', 'KIC-103', '2023-01-01 04:00:00', 5000.0, 0),
('LC-047', 'KIC-103', '2023-01-01 05:00:00', 5000.0, 0),
('LC-048', 'KIC-103', '2023-01-01 06:00:00', 5000.0, 0),
('LC-049', 'KIC-103', '2023-01-01 07:00:00', 5000.0, 0),
('LC-050', 'KIC-103', '2023-01-01 08:00:00', 5000.0, 0);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Astro_Physics_Lab.Bronze.Bronze_Stars AS SELECT * FROM Astro_Physics_Lab.Sources.Target_Stars;
CREATE OR REPLACE VIEW Astro_Physics_Lab.Bronze.Bronze_Curves AS SELECT * FROM Astro_Physics_Lab.Sources.Light_Curves;

-- 4b. SILVER LAYER (Pre-processed Flux)
-- Normalize Flux to 1.0 baseline for easier comparison
CREATE OR REPLACE VIEW Astro_Physics_Lab.Silver.Silver_Normalized_Flux AS
SELECT
    l.KeplerID,
    l.Observation_Time,
    l.Flux_Electron_Count,
    -- Calculate Baseline per star (Window function)
    AVG(l.Flux_Electron_Count) OVER (PARTITION BY l.KeplerID) as Avg_Flux,
    l.Flux_Electron_Count / AVG(l.Flux_Electron_Count) OVER (PARTITION BY l.KeplerID) as Normalized_Flux,
    l.Quality_Flag
FROM Astro_Physics_Lab.Bronze.Bronze_Curves l
WHERE l.Quality_Flag = 0; -- Filter noise

-- 4c. GOLD LAYER (Transit Candidates)
CREATE OR REPLACE VIEW Astro_Physics_Lab.Gold.Gold_Candidate_List AS
SELECT
    KeplerID,
    COUNT(Observation_Time) as Data_Points,
    MIN(Normalized_Flux) as Max_Dip_Depth,
    -- Simple Transit Flag: Dip > 1% (0.99)
    CASE 
        WHEN MIN(Normalized_Flux) < 0.99 THEN 'Strong Candidate'
        WHEN MIN(Normalized_Flux) < 0.995 THEN 'Possible Candidate'
        ELSE 'No Transit Detected'
    END as Status
FROM Astro_Physics_Lab.Silver.Silver_Normalized_Flux
GROUP BY KeplerID;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Silver_Normalized_Flux' for 'KIC-101'. 
 * Identify the time intervals between dips < 0.99 normalized flux to estimate the orbital period."
 */
