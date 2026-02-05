/*
 * Dremio Data Center Immersion Cooling Example
 * 
 * Domain: Tech Infrastructure & Green Energy
 * Scenario: 
 * A Data Center uses "Immersion Cooling" (servers dipped in dielectric fluid).
 * We monitor "Fluid Temperature", "Pump Flow Rate", and "Server Load".
 * The goal is to optimize 'PUE' (Power Usage Effectiveness) by running pumps only as needed.
 * 
 * Complexity: Medium (Thermal dynamics, efficiency ratios)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Server_Immersion;
CREATE FOLDER IF NOT EXISTS Server_Immersion.Sources;
CREATE FOLDER IF NOT EXISTS Server_Immersion.Bronze;
CREATE FOLDER IF NOT EXISTS Server_Immersion.Silver;
CREATE FOLDER IF NOT EXISTS Server_Immersion.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Server_Immersion.Sources.Tank_Config (
    TankID VARCHAR,
    Capacity_Kw INT, -- 250kW tank
    Fluid_Type VARCHAR -- 'Mineral Oil', 'Engineered Fluid'
);

CREATE TABLE IF NOT EXISTS Server_Immersion.Sources.Cooling_Telemetry (
    LogID VARCHAR,
    TankID VARCHAR,
    Reading_Timestamp TIMESTAMP,
    Fluid_Temp_In_C DOUBLE,
    Fluid_Temp_Out_C DOUBLE,
    Pump_Flow_GPM INT,
    IT_Load_Kw DOUBLE, -- Server power
    Facility_Power_Kw DOUBLE -- Total power (IT + Cooling)
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Tanks
INSERT INTO Server_Immersion.Sources.Tank_Config VALUES
('TANK-01', 250, 'Engineered Fluid'),
('TANK-02', 250, 'Mineral Oil');

-- Seed Telemetry
-- PUE = Facility_Power / IT_Load. Ideal is 1.02 for immersion.
INSERT INTO Server_Immersion.Sources.Cooling_Telemetry VALUES
('TLM-001', 'TANK-01', '2023-11-01 08:00:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-002', 'TANK-01', '2023-11-01 08:05:00', 45.5, 51.0, 100, 210.0, 216.0),
('TLM-003', 'TANK-01', '2023-11-01 08:10:00', 46.0, 52.0, 110, 220.0, 227.0), -- Pump ramp up
('TLM-004', 'TANK-01', '2023-11-01 08:15:00', 45.0, 50.0, 110, 200.0, 208.0), -- Cooled down
('TLM-005', 'TANK-02', '2023-11-01 08:00:00', 40.0, 48.0, 80, 150.0, 160.0), -- Oil runs hotter delta?
('TLM-006', 'TANK-02', '2023-11-01 08:05:00', 40.5, 49.0, 80, 155.0, 165.0),
-- Fill 50
('TLM-007', 'TANK-01', '2023-11-01 08:20:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-008', 'TANK-01', '2023-11-01 08:25:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-009', 'TANK-01', '2023-11-01 08:30:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-010', 'TANK-01', '2023-11-01 08:35:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-011', 'TANK-01', '2023-11-01 08:40:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-012', 'TANK-01', '2023-11-01 08:45:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-013', 'TANK-01', '2023-11-01 08:50:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-014', 'TANK-01', '2023-11-01 08:55:00', 45.0, 50.0, 100, 200.0, 205.0),
('TLM-015', 'TANK-01', '2023-11-01 09:00:00', 50.0, 60.0, 100, 240.0, 250.0), -- Hot spot!
('TLM-016', 'TANK-01', '2023-11-01 09:05:00', 48.0, 55.0, 150, 240.0, 255.0), -- Flow increased
('TLM-017', 'TANK-01', '2023-11-01 09:10:00', 46.0, 51.0, 150, 240.0, 255.0), -- Stabilizing
('TLM-018', 'TANK-02', '2023-11-01 08:10:00', 41.0, 50.0, 80, 160.0, 170.0),
('TLM-019', 'TANK-02', '2023-11-01 08:15:00', 41.5, 51.0, 80, 165.0, 175.0),
('TLM-020', 'TANK-02', '2023-11-01 08:20:00', 42.0, 52.0, 80, 170.0, 180.0),
('TLM-021', 'TANK-02', '2023-11-01 08:25:00', 42.0, 52.0, 90, 170.0, 181.0), -- Pump up
('TLM-022', 'TANK-02', '2023-11-01 08:30:00', 41.0, 50.0, 90, 170.0, 181.0),
('TLM-023', 'TANK-02', '2023-11-01 08:35:00', 40.0, 48.0, 90, 150.0, 161.0),
('TLM-024', 'TANK-02', '2023-11-01 08:40:00', 40.0, 48.0, 80, 150.0, 160.0),
('TLM-025', 'TANK-02', '2023-11-01 08:45:00', 40.0, 48.0, 80, 150.0, 160.0),
('TLM-026', 'TANK-01', '2023-11-02 08:00:00', 45, 50, 100, 200, 205),
('TLM-027', 'TANK-01', '2023-11-02 08:05:00', 45, 50, 100, 200, 205),
('TLM-028', 'TANK-01', '2023-11-02 08:10:00', 45, 50, 100, 200, 205),
('TLM-029', 'TANK-01', '2023-11-02 08:15:00', 45, 50, 100, 200, 205),
('TLM-030', 'TANK-01', '2023-11-02 08:20:00', 45, 50, 100, 200, 205),
('TLM-031', 'TANK-01', '2023-11-02 08:25:00', 45, 50, 100, 200, 205),
('TLM-032', 'TANK-01', '2023-11-02 08:30:00', 45, 50, 100, 200, 205),
('TLM-033', 'TANK-01', '2023-11-02 08:35:00', 45, 50, 100, 200, 205),
('TLM-034', 'TANK-01', '2023-11-02 08:40:00', 45, 50, 100, 200, 205),
('TLM-035', 'TANK-01', '2023-11-02 08:45:00', 45, 50, 100, 200, 205),
('TLM-036', 'TANK-02', '2023-11-02 08:00:00', 40, 48, 80, 150, 160),
('TLM-037', 'TANK-02', '2023-11-02 08:05:00', 40, 48, 80, 150, 160),
('TLM-038', 'TANK-02', '2023-11-02 08:10:00', 40, 48, 80, 150, 160),
('TLM-039', 'TANK-02', '2023-11-02 08:15:00', 40, 48, 80, 150, 160),
('TLM-040', 'TANK-02', '2023-11-02 08:20:00', 40, 48, 80, 150, 160),
('TLM-041', 'TANK-02', '2023-11-02 08:25:00', 40, 48, 80, 150, 160),
('TLM-042', 'TANK-02', '2023-11-02 08:30:00', 40, 48, 80, 150, 160),
('TLM-043', 'TANK-02', '2023-11-02 08:35:00', 40, 48, 80, 150, 160),
('TLM-044', 'TANK-02', '2023-11-02 08:40:00', 40, 48, 80, 150, 160),
('TLM-045', 'TANK-02', '2023-11-02 08:45:00', 40, 48, 80, 150, 160),
('TLM-046', 'TANK-01', '2023-11-03 08:00:00', 50, 60, 100, 240, 250),
('TLM-047', 'TANK-01', '2023-11-03 08:05:00', 50, 60, 150, 240, 255),
('TLM-048', 'TANK-01', '2023-11-03 08:10:00', 48, 55, 150, 240, 255),
('TLM-049', 'TANK-01', '2023-11-03 08:15:00', 46, 51, 150, 240, 255),
('TLM-050', 'TANK-01', '2023-11-03 08:20:00', 45, 50, 110, 240, 248);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Server_Immersion.Bronze.Bronze_Tanks AS SELECT * FROM Server_Immersion.Sources.Tank_Config;
CREATE OR REPLACE VIEW Server_Immersion.Bronze.Bronze_Telemetry AS SELECT * FROM Server_Immersion.Sources.Cooling_Telemetry;

-- 4b. SILVER LAYER (Efficiency Calculations)
CREATE OR REPLACE VIEW Server_Immersion.Silver.Silver_Thermal_Efficiency AS
SELECT
    t.LogID,
    t.TankID,
    t.Reading_Timestamp,
    t.Fluid_Temp_In_C,
    t.Fluid_Temp_Out_C,
    t.Pump_Flow_GPM,
    -- PUE Calculation
    ROUND(t.Facility_Power_Kw / NULLIF(t.IT_Load_Kw, 0), 3) as PUE_Instant,
    -- Thermal Delta
    (t.Fluid_Temp_Out_C - t.Fluid_Temp_In_C) as Delta_T_Celsius
FROM Server_Immersion.Bronze.Bronze_Telemetry t;

-- 4c. GOLD LAYER (Optimization Targets)
CREATE OR REPLACE VIEW Server_Immersion.Gold.Gold_PUE_Optimization AS
SELECT
    TankID,
    AVG(PUE_Instant) as Avg_PUE,
    MAX(Fluid_Temp_Out_C) as Max_Temp_Reached,
    AVG(Delta_T_Celsius) as Avg_Delta_T,
    -- Tuning Action
    CASE 
        WHEN AVG(PUE_Instant) > 1.05 THEN 'Reduce Pump Speed'
        WHEN MAX(Fluid_Temp_Out_C) > 55 THEN 'Increase Flow'
        ELSE 'Optimal'
    END as Tuning_Recommendation
FROM Server_Immersion.Silver.Silver_Thermal_Efficiency
GROUP BY TankID;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "From 'Gold_PUE_Optimization', identify tanks with 'Reduce Pump Speed' recommendation. 
 * This indicates over-cooling where energy is wasted."
 */
