/*
 * Dremio Astronomy Radio Telescope Example
 * 
 * Domain: Astrophysics & High-Performance Computing
 * Scenario: 
 * A Radio Telescope array captures massive streaming data. Scientists need to 
 * detect meaningful "Transient Events" (like Fast Radio Bursts - FRBs) while filtering 
 * out RFI (Radio Frequency Interference) from satellites (e.g., Starlink) and aircraft.
 * This example simulates the "Subtraction" process where "Interference Logs" are used 
 * to clean the raw "Signal Stream".
 * 
 * Complexity: High (Signal-to-noise logic, range-based exclusion joins)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Radio_Astro;
CREATE FOLDER IF NOT EXISTS Radio_Astro.Sources;
CREATE FOLDER IF NOT EXISTS Radio_Astro.Bronze;
CREATE FOLDER IF NOT EXISTS Radio_Astro.Silver;
CREATE FOLDER IF NOT EXISTS Radio_Astro.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Radio_Astro.Sources.Interference_Logs (
    LogID VARCHAR,
    Source_Type VARCHAR, -- 'Satellite', 'Microwave', 'Aircraft'
    Frequency_Start_MHz DOUBLE,
    Frequency_End_MHz DOUBLE,
    Active_Time_Start TIMESTAMP,
    Active_Time_End TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Radio_Astro.Sources.Signal_Stream (
    SignalID VARCHAR,
    Observation_Target VARCHAR, -- 'Crab Pulsar', 'M87', 'Kepler-16b'
    Timestamp TIMESTAMP,
    Frequency_MHz DOUBLE,
    Amplitude_Jy DOUBLE, -- Janskys
    Signal_Width_ms DOUBLE -- Milliseconds
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Interference (Starlink passes, Airplanes)
INSERT INTO Radio_Astro.Sources.Interference_Logs VALUES
('RFI-001', 'Satellite', 1400.0, 1420.0, '2023-11-01 00:00:00', '2023-11-01 04:00:00'),
('RFI-002', 'Satellite', 1400.0, 1420.0, '2023-11-01 08:00:00', '2023-11-01 12:00:00'),
('RFI-003', 'Aircraft', 108.0, 137.0, '2023-11-01 10:00:00', '2023-11-01 10:30:00'),
('RFI-004', 'Microwave', 2400.0, 2500.0, '2023-11-01 12:00:00', '2023-11-01 13:00:00'), -- Lunch break interference!
('RFI-005', 'Satellite', 1600.0, 1610.0, '2023-11-01 00:00:00', '2023-11-01 23:59:59'),
('RFI-006', 'GPS', 1575.0, 1580.0, '2023-11-01 00:00:00', '2023-11-01 23:59:59'),
('RFI-007', 'Aircraft', 108.0, 137.0, '2023-11-01 14:00:00', '2023-11-01 14:15:00'),
('RFI-008', 'Radar', 2700.0, 2900.0, '2023-11-01 00:00:00', '2023-11-01 23:59:59'),
('RFI-009', 'Satellite', 1400.0, 1420.0, '2023-11-01 20:00:00', '2023-11-01 22:00:00'),
('RFI-010', 'Microwave', 2400.0, 2500.0, '2023-11-01 18:00:00', '2023-11-01 19:00:00'),
('RFI-011', 'GSM', 900.0, 915.0, '2023-11-01 00:00:00', '2023-11-01 23:59:59'),
('RFI-012', 'GSM', 1800.0, 1815.0, '2023-11-01 00:00:00', '2023-11-01 23:59:59'),
('RFI-013', 'Aircraft', 108.0, 137.0, '2023-11-01 16:00:00', '2023-11-01 16:45:00'),
('RFI-014', 'Satellite', 1200.0, 1220.0, '2023-11-01 03:00:00', '2023-11-01 05:00:00'),
('RFI-015', 'Satellite', 1200.0, 1220.0, '2023-11-01 15:00:00', '2023-11-01 17:00:00');

-- Seed Signals (Mix of noise, known pulsars, and possible FRBs)
-- Target: Crab Pulsar (30 Hz ~ 33ms period), signal should appear periodically
INSERT INTO Radio_Astro.Sources.Signal_Stream VALUES
('SIG-001', 'Crab Pulsar', '2023-11-01 00:00:00.000', 1410.0, 50.0, 1.2), -- Interference!
('SIG-002', 'Crab Pulsar', '2023-11-01 00:00:00.033', 1410.0, 52.0, 1.3), -- Interference
('SIG-003', 'Crab Pulsar', '2023-11-01 00:00:00.066', 1410.0, 48.0, 1.1), -- Interference
('SIG-004', 'Crab Pulsar', '2023-11-01 05:00:00.000', 1425.0, 10.0, 1.2), -- Clean?
('SIG-005', 'Crab Pulsar', '2023-11-01 05:00:00.033', 1425.0, 12.0, 1.3),
('SIG-006', 'Crab Pulsar', '2023-11-01 05:00:00.066', 1425.0, 0.5, 99.0), -- Noise
('SIG-007', 'M87', '2023-11-01 10:15:00.000', 120.0, 60.0, 5.0), -- Aircraft RFI
('SIG-008', 'M87', '2023-11-01 12:30:00.000', 2450.0, 100.0, 20.0), -- Microwave RFI
('SIG-009', 'Kepler-16b', '2023-11-01 23:00:00.000', 1420.4, 0.2, 5.0), -- Hydrogen line?
('SIG-010', 'Unknown', '2023-11-01 23:45:00.000', 1500.0, 500.0, 0.5), -- FRB Candidate?
-- Bulk inserts
('SIG-011', 'Crab Pulsar', '2023-11-01 05:00:00.099', 1425.0, 11.0, 1.2),
('SIG-012', 'Crab Pulsar', '2023-11-01 05:00:00.132', 1425.0, 10.5, 1.25),
('SIG-013', 'Crab Pulsar', '2023-11-01 05:00:00.165', 1425.0, 0.8, 80.0), -- Noise
('SIG-014', 'Crab Pulsar', '2023-11-01 05:00:00.198', 1425.0, 12.0, 1.2),
('SIG-015', 'Crab Pulsar', '2023-11-01 05:00:00.231', 1425.0, 11.5, 1.3),
('SIG-016', 'Blank Sky', '2023-11-01 06:00:00', 1420.0, 0.1, 100.0), -- Background
('SIG-017', 'Blank Sky', '2023-11-01 06:01:00', 1420.0, 0.1, 100.0),
('SIG-018', 'Blank Sky', '2023-11-01 06:02:00', 1420.0, 0.1, 100.0),
('SIG-019', 'Blank Sky', '2023-11-01 06:03:00', 1420.0, 0.1, 100.0),
('SIG-020', 'Blank Sky', '2023-11-01 06:04:00', 1420.0, 0.1, 100.0),
('SIG-021', 'Unknown', '2023-11-01 02:00:00', 1577.0, 40.0, 1.0), -- GPS RFI
('SIG-022', 'Unknown', '2023-11-01 02:05:00', 1578.0, 42.0, 1.1),
('SIG-023', 'Unknown', '2023-11-01 02:10:00', 1579.0, 41.0, 1.2),
('SIG-024', 'Low Band', '2023-11-01 10:10:00', 110.0, 55.0, 10.0), -- Aircraft
('SIG-025', 'Low Band', '2023-11-01 16:10:00', 115.0, 50.0, 10.0), -- Aircraft
('SIG-026', 'Unknown', '2023-11-01 04:00:00', 1415.0, 50.0, 1.2), -- Edge of Sat RFI
('SIG-027', 'Unknown', '2023-11-01 23:45:01', 1500.0, 450.0, 0.6), -- FRB Echo?
('SIG-028', 'Unknown', '2023-11-01 23:45:02', 1500.0, 100.0, 1.0),
('SIG-029', 'Kepler-16b', '2023-11-01 23:01:00', 1420.4, 0.2, 5.0),
('SIG-030', 'Kepler-16b', '2023-11-01 23:02:00', 1420.4, 0.2, 5.0),
('SIG-031', 'Kepler-16b', '2023-11-01 23:03:00', 1420.4, 0.3, 5.0), -- Drift
('SIG-032', 'Kepler-16b', '2023-11-01 23:04:00', 1420.4, 0.2, 5.0),
('SIG-033', 'Kepler-16b', '2023-11-01 23:05:00', 1420.4, 0.2, 5.0),
('SIG-034', 'High Band', '2023-11-01 12:00:00', 2800.0, 80.0, 20.0), -- Radar
('SIG-035', 'High Band', '2023-11-01 13:00:00', 2850.0, 85.0, 20.0),
('SIG-036', 'High Band', '2023-11-01 14:00:00', 2750.0, 75.0, 20.0),
('SIG-037', 'High Band', '2023-11-01 15:00:00', 2900.0, 90.0, 20.0),
('SIG-038', 'Burst', '2023-11-01 00:00:05', 800.0, 2000.0, 0.1), -- Low freq burst
('SIG-039', 'Burst', '2023-11-01 00:00:06', 700.0, 1500.0, 0.2), -- Dispersion tail?
('SIG-040', 'Burst', '2023-11-01 00:00:07', 600.0, 1000.0, 0.3),
('SIG-041', 'Burst', '2023-11-01 00:00:08', 500.0, 800.0, 0.4),
('SIG-042', 'Burst', '2023-11-01 00:00:09', 400.0, 600.0, 0.5), -- Classic dispersion sweep!
('SIG-043', 'S1', '2023-11-01 10:00:00', 905.0, 20.0, 5.0), -- GSM
('SIG-044', 'S1', '2023-11-01 10:01:00', 910.0, 22.0, 5.0),
('SIG-045', 'S2', '2023-11-01 11:00:00', 1805.0, 20.0, 5.0), -- GSM
('SIG-046', 'S2', '2023-11-01 11:01:00', 1810.0, 22.0, 5.0),
('SIG-047', 'S3', '2023-11-01 18:30:00', 2450.0, 95.0, 15.0), -- Microwave
('SIG-048', 'S3', '2023-11-01 18:31:00', 2460.0, 98.0, 15.0),
('SIG-049', 'Titan', '2023-11-01 20:00:00', 1410.0, 5.0, 1000.0), -- Slow signal
('SIG-050', 'Titan', '2023-11-01 20:10:00', 1410.0, 5.1, 1000.0);


-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Radio_Astro.Bronze.Bronze_Signals AS
SELECT * FROM Radio_Astro.Sources.Signal_Stream;

CREATE OR REPLACE VIEW Radio_Astro.Bronze.Bronze_RFI AS
SELECT * FROM Radio_Astro.Sources.Interference_Logs;

-- 4b. SILVER LAYER (RFI Subtraction)
CREATE OR REPLACE VIEW Radio_Astro.Silver.Silver_Clean_Signals AS
SELECT
    s.SignalID,
    s.Observation_Target,
    s.Timestamp,
    s.Frequency_MHz,
    s.Amplitude_Jy,
    s.Signal_Width_ms,
    CASE 
        WHEN r.LogID IS NOT NULL THEN 'RFI Contaminated'
        ELSE 'Clean'
    END as Quality_Flag,
    r.Source_Type as Interference_Source
FROM Radio_Astro.Bronze.Bronze_Signals s
LEFT JOIN Radio_Astro.Bronze.Bronze_RFI r
  ON s.Frequency_MHz >= r.Frequency_Start_MHz
  AND s.Frequency_MHz <= r.Frequency_End_MHz
  AND s.Timestamp >= r.Active_Time_Start
  AND s.Timestamp <= r.Active_Time_End;

-- 4c. GOLD LAYER (Transient Detection)
CREATE OR REPLACE VIEW Radio_Astro.Gold.Gold_Fast_Radio_Bursts AS
SELECT
    SignalID,
    Timestamp,
    Frequency_MHz,
    Amplitude_Jy,
    Signal_Width_ms
FROM Radio_Astro.Silver.Silver_Clean_Signals
WHERE Quality_Flag = 'Clean'
AND Amplitude_Jy > 100.0 -- High energy
AND Signal_Width_ms < 10.0; -- Very short duration

CREATE OR REPLACE VIEW Radio_Astro.Gold.Gold_Pulsar_Candidates AS
SELECT
    Observation_Target,
    COUNT(*) as Pulse_Count,
    AVG(Amplitude_Jy) as Avg_Intensity,
    STDDEV(Amplitude_Jy) as Intensity_Variability
FROM Radio_Astro.Silver.Silver_Clean_Signals
WHERE Quality_Flag = 'Clean'
AND Signal_Width_ms BETWEEN 1.0 AND 50.0
GROUP BY Observation_Target
HAVING COUNT(*) > 5;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Silver_Clean_Signals' where Quality_Flag = 'Clean'. 
 * Look for 'Dispersion Sweeps' (high amplitude signals appearing sequentially at lower frequencies over time, like SIG-038 to SIG-042)."
 */
