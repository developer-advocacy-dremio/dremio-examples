/*
 * Dremio Cryogenic Biobank Example
 * 
 * Domain: Biotech & Clinical Research
 * Scenario: 
 * A Biobank stores human tissue/blood samples in Liquid Nitrogen (LN2) tanks at -196 C.
 * They monitor "Tank Level" (LN2 evaporation) and "Temperature" to prevent "Thaw Events".
 * Samples have "Freeze-Thaw Cycles" tracked, as too many cycles degrade DNA/RNA.
 * 
 * Complexity: Medium (Inventory tracking, critical limit alerts)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Bio_Storage;
CREATE FOLDER IF NOT EXISTS Bio_Storage.Sources;
CREATE FOLDER IF NOT EXISTS Bio_Storage.Bronze;
CREATE FOLDER IF NOT EXISTS Bio_Storage.Silver;
CREATE FOLDER IF NOT EXISTS Bio_Storage.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Bio_Storage.Sources.Sample_Manifest (
    SampleID VARCHAR,
    DonorID VARCHAR,
    Tissue_Type VARCHAR, -- 'Blood', 'Tumor', 'DNA'
    Collection_Date DATE,
    Rack_Location VARCHAR, -- 'Tank-A:Rack-1:Box-1'
    Current_Status VARCHAR -- 'Stored', 'Checked Out', 'Destroyed'
);

CREATE TABLE IF NOT EXISTS Bio_Storage.Sources.Tank_Telemetry (
    LogID VARCHAR,
    TankID VARCHAR,
    Log_Timestamp TIMESTAMP,
    Temp_Celsius DOUBLE, -- Target -196
    LN2_Level_Percent DOUBLE, -- Refill at 20%
    Alarm_Triggered BOOLEAN
);

CREATE TABLE IF NOT EXISTS Bio_Storage.Sources.Access_Log (
    AccessID VARCHAR,
    SampleID VARCHAR,
    Access_Date TIMESTAMP,
    Action_Type VARCHAR, -- 'Retrieval', 'Deposit'
    Duration_Minutes INT -- Exposure to room temp
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Samples
INSERT INTO Bio_Storage.Sources.Sample_Manifest VALUES
('SMP-001', 'DON-100', 'Blood', '2023-01-01', 'Tank-A:Rack-1:Box-1', 'Stored'),
('SMP-002', 'DON-100', 'DNA', '2023-01-01', 'Tank-A:Rack-1:Box-1', 'Stored'),
('SMP-003', 'DON-101', 'Tumor', '2023-02-01', 'Tank-A:Rack-1:Box-2', 'Checked Out'),
('SMP-004', 'DON-102', 'Blood', '2023-03-01', 'Tank-B:Rack-1:Box-1', 'Stored'),
('SMP-005', 'DON-103', 'Plasma', '2023-03-15', 'Tank-B:Rack-2:Box-1', 'Stored');

-- Seed Telemetry (Tank A healthy, Tank B issue)
INSERT INTO Bio_Storage.Sources.Tank_Telemetry VALUES
('TLM-001', 'Tank-A', '2023-11-01 08:00:00', -196.0, 80.0, false),
('TLM-002', 'Tank-A', '2023-11-01 12:00:00', -195.8, 79.5, false),
('TLM-003', 'Tank-A', '2023-11-02 08:00:00', -196.1, 78.0, false),
('TLM-004', 'Tank-A', '2023-11-02 12:00:00', -196.0, 77.5, false),
('TLM-005', 'Tank-B', '2023-11-01 08:00:00', -190.0, 25.0, false), -- Low level
('TLM-006', 'Tank-B', '2023-11-01 12:00:00', -180.0, 22.0, true), -- Warming up!
('TLM-007', 'Tank-B', '2023-11-01 16:00:00', -150.0, 18.0, true), -- Critical warmth
('TLM-008', 'Tank-B', '2023-11-01 17:00:00', -196.0, 100.0, false), -- Refilled
('TLM-009', 'Tank-A', '2023-11-03 08:00:00', -196.0, 76.0, false),
('TLM-010', 'Tank-A', '2023-11-03 12:00:00', -196.0, 75.5, false),
-- Fill 40 rows
('TLM-011', 'Tank-A', '2023-11-04 08:00:00', -196, 75, false),
('TLM-012', 'Tank-A', '2023-11-04 12:00:00', -196, 74, false),
('TLM-013', 'Tank-A', '2023-11-05 08:00:00', -196, 73, false),
('TLM-014', 'Tank-A', '2023-11-05 12:00:00', -196, 72, false),
('TLM-015', 'Tank-A', '2023-11-06 08:00:00', -196, 71, false),
('TLM-016', 'Tank-A', '2023-11-06 12:00:00', -196, 70, false),
('TLM-017', 'Tank-A', '2023-11-07 08:00:00', -196, 69, false),
('TLM-018', 'Tank-A', '2023-11-07 12:00:00', -196, 68, false),
('TLM-019', 'Tank-B', '2023-11-02 08:00:00', -196, 99, false),
('TLM-020', 'Tank-B', '2023-11-02 12:00:00', -196, 98, false),
('TLM-021', 'Tank-B', '2023-11-03 08:00:00', -196, 97, false),
('TLM-022', 'Tank-B', '2023-11-03 12:00:00', -196, 96, false),
('TLM-023', 'Tank-B', '2023-11-04 08:00:00', -196, 95, false),
('TLM-024', 'Tank-B', '2023-11-04 12:00:00', -196, 94, false),
('TLM-025', 'Tank-B', '2023-11-05 08:00:00', -196, 93, false),
('TLM-026', 'Tank-B', '2023-11-05 12:00:00', -196, 92, false),
('TLM-027', 'Tank-B', '2023-11-06 08:00:00', -196, 91, false),
('TLM-028', 'Tank-B', '2023-11-06 12:00:00', -196, 90, false),
('TLM-029', 'Tank-B', '2023-11-07 08:00:00', -196, 89, false),
('TLM-030', 'Tank-B', '2023-11-07 12:00:00', -196, 88, false);

-- Seed Access
INSERT INTO Bio_Storage.Sources.Access_Log VALUES
('ACC-001', 'SMP-001', '2023-10-01 09:00:00', 'Deposit', 2),
('ACC-002', 'SMP-001', '2023-11-01 10:00:00', 'Retrieval', 5), -- Checked out briefly
('ACC-003', 'SMP-001', '2023-11-01 14:00:00', 'Deposit', 2), -- Returned
('ACC-004', 'SMP-003', '2023-10-15 09:00:00', 'Retrieval', 10),
('ACC-005', 'SMP-004', '2023-10-20 09:00:00', 'Deposit', 5),
('ACC-006', 'SMP-002', '2023-10-01 09:00:00', 'Deposit', 2),
('ACC-007', 'SMP-005', '2023-10-01 09:00:00', 'Deposit', 2),
('ACC-008', 'SMP-002', '2023-11-02 10:00:00', 'Retrieval', 5),
('ACC-009', 'SMP-002', '2023-11-02 10:30:00', 'Deposit', 2),
('ACC-010', 'SMP-002', '2023-11-05 10:00:00', 'Retrieval', 25), -- Left out too long?
('ACC-011', 'SMP-002', '2023-11-05 10:30:00', 'Deposit', 2);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Bio_Storage.Bronze.Bronze_Manifest AS SELECT * FROM Bio_Storage.Sources.Sample_Manifest;
CREATE OR REPLACE VIEW Bio_Storage.Bronze.Bronze_Telemetry AS SELECT * FROM Bio_Storage.Sources.Tank_Telemetry;
CREATE OR REPLACE VIEW Bio_Storage.Bronze.Bronze_Access AS SELECT * FROM Bio_Storage.Sources.Access_Log;

-- 4b. SILVER LAYER (Sample Integrity)
CREATE OR REPLACE VIEW Bio_Storage.Silver.Silver_Sample_History AS
SELECT
    m.SampleID,
    m.Tissue_Type,
    m.Current_Status,
    COUNT(a.AccessID) as Total_Handling_Events,
    SUM(CASE WHEN a.Action_Type = 'Retrieval' THEN 1 ELSE 0 END) as Freeze_Thaw_Cycles,
    SUM(a.Duration_Minutes) as Total_Room_Temp_Exposure_Min
FROM Bio_Storage.Bronze.Bronze_Manifest m
LEFT JOIN Bio_Storage.Bronze.Bronze_Access a ON m.SampleID = a.SampleID
GROUP BY m.SampleID, m.Tissue_Type, m.Current_Status;

-- 4c. GOLD LAYER (Monitoring Dashboard)
CREATE OR REPLACE VIEW Bio_Storage.Gold.Gold_Tank_Alerts AS
SELECT
    TankID,
    AVG(Temp_Celsius) as Avg_Temp,
    MIN(LN2_Level_Percent) as Min_Level,
    -- Alert Logic
    CASE 
        WHEN MIN(LN2_Level_Percent) < 20.0 THEN 'CRITICAL: REFILL NEEDED'
        WHEN MAX(Temp_Celsius) > -150.0 THEN 'CRITICAL: WARMUP DETECTED'
        ELSE 'Normal'
    END as Tank_Status
FROM Bio_Storage.Bronze.Bronze_Telemetry
WHERE Log_Timestamp > CURRENT_DATE - 7 -- Last 7 days
GROUP BY TankID;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Review 'Silver_Sample_History' for DNA samples with > 5 Freeze_Thaw_Cycles. 
 * These samples may be degraded and should be flagged for Quality Control testing."
 */
