/*
 * Dremio Desalination Plant Operations Example
 * 
 * Domain: Utilities & Water Management
 * Scenario: 
 * A Reverse Osmosis (RO) plant purifies seawater.
 * Sensors track "Feed Pressure", "Permeate Flow" (Clean water), and "Brine Reject" (Concentrated salt).
 * The goal is to monitor "Membrane Fouling" (clogging) by analyzing pressure differentials 
 * and energy usage (kWh/m3) to schedule cleaning cycles (CIP).
 * 
 * Complexity: Medium (Energy efficiency metrics, differential calculations)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Hydro_Ops;
CREATE FOLDER IF NOT EXISTS Hydro_Ops.Sources;
CREATE FOLDER IF NOT EXISTS Hydro_Ops.Bronze;
CREATE FOLDER IF NOT EXISTS Hydro_Ops.Silver;
CREATE FOLDER IF NOT EXISTS Hydro_Ops.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Hydro_Ops.Sources.RO_Trains (
    TrainID VARCHAR,
    Install_Date DATE,
    Membrane_Type VARCHAR, -- 'SWRO-HighReject', 'SWRO-LowEnergy'
    Max_Capacity_m3_Day INT,
    Last_CIP_Date DATE -- Clean In Place
);

CREATE TABLE IF NOT EXISTS Hydro_Ops.Sources.Sensor_Telemetry (
    ReadingID VARCHAR,
    TrainID VARCHAR,
    Timestamp TIMESTAMP,
    Feed_Pressure_Bar DOUBLE,
    Permeate_Flow_m3_Hr DOUBLE,
    Conductivity_uS_cm INT, -- Water purity (lower is better)
    Power_Consumption_kW DOUBLE,
    Feed_Salinity_TDS INT -- Total Dissolved Solids
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Trains (Production Lines)
INSERT INTO Hydro_Ops.Sources.RO_Trains VALUES
('TRAIN-A', '2020-01-01', 'SWRO-HighReject', 5000, '2023-10-01'),
('TRAIN-B', '2020-06-01', 'SWRO-HighReject', 5000, '2023-09-15'),
('TRAIN-C', '2021-03-01', 'SWRO-LowEnergy', 6000, '2023-11-01');

-- Seed Telemetry
-- Normal Op: Pressure ~60 Bar, Flow ~200 m3/h, Conductivity < 400
-- Fouling: Pressure increases to maintain flow, or flow drops.
INSERT INTO Hydro_Ops.Sources.Sensor_Telemetry VALUES
('RD-100', 'TRAIN-A', '2023-11-01 08:00:00', 60.0, 200.0, 350, 800.0, 35000),
('RD-101', 'TRAIN-A', '2023-11-01 09:00:00', 60.1, 200.0, 350, 801.0, 35000),
('RD-102', 'TRAIN-A', '2023-11-01 10:00:00', 60.2, 199.5, 355, 802.0, 35100),
('RD-103', 'TRAIN-A', '2023-11-01 11:00:00', 60.3, 199.5, 355, 803.0, 35200),
('RD-104', 'TRAIN-A', '2023-11-02 08:00:00', 62.0, 198.0, 360, 820.0, 35000), -- Fouling starts
('RD-105', 'TRAIN-A', '2023-11-02 09:00:00', 62.2, 198.0, 362, 822.0, 35000),
('RD-106', 'TRAIN-A', '2023-11-02 10:00:00', 62.5, 197.5, 365, 825.0, 35000),
('RD-107', 'TRAIN-A', '2023-11-02 11:00:00', 62.8, 197.0, 368, 830.0, 35000),
('RD-108', 'TRAIN-A', '2023-11-03 08:00:00', 65.0, 190.0, 400, 850.0, 35000), -- Bad fouling
('RD-109', 'TRAIN-A', '2023-11-03 09:00:00', 65.5, 185.0, 410, 860.0, 35000),
('RD-110', 'TRAIN-B', '2023-11-01 08:00:00', 58.0, 210.0, 300, 750.0, 35000), -- Fresh train
('RD-111', 'TRAIN-B', '2023-11-01 09:00:00', 58.0, 210.0, 300, 750.0, 35000),
('RD-112', 'TRAIN-B', '2023-11-01 10:00:00', 58.1, 210.0, 305, 751.0, 35000),
('RD-113', 'TRAIN-B', '2023-11-01 11:00:00', 58.1, 209.5, 305, 752.0, 35000),
('RD-114', 'TRAIN-C', '2023-11-01 08:00:00', 55.0, 240.0, 280, 700.0, 35000), -- Low Energy membranes
('RD-115', 'TRAIN-C', '2023-11-01 09:00:00', 55.0, 240.0, 280, 700.0, 35000),
('RD-116', 'TRAIN-C', '2023-11-01 10:00:00', 55.2, 239.0, 282, 705.0, 35000),
('RD-117', 'TRAIN-C', '2023-11-01 11:00:00', 55.2, 239.0, 282, 705.0, 35000),
('RD-118', 'TRAIN-A', '2023-11-03 10:00:00', 66.0, 180.0, 420, 870.0, 35000), -- Critical cleaning needed
('RD-119', 'TRAIN-A', '2023-11-03 11:00:00', 66.5, 175.0, 430, 880.0, 35000),
('RD-120', 'TRAIN-B', '2023-11-02 08:00:00', 58.5, 208.0, 310, 760.0, 35000),
('RD-121', 'TRAIN-B', '2023-11-02 09:00:00', 58.6, 208.0, 312, 762.0, 35000),
('RD-122', 'TRAIN-B', '2023-11-02 10:00:00', 58.7, 207.5, 315, 765.0, 35000),
('RD-123', 'TRAIN-B', '2023-11-02 11:00:00', 58.8, 207.0, 318, 770.0, 35000),
('RD-124', 'TRAIN-B', '2023-11-03 08:00:00', 59.0, 205.0, 320, 780.0, 35000),
('RD-125', 'TRAIN-C', '2023-11-02 08:00:00', 55.5, 238.0, 285, 710.0, 35000),
('RD-126', 'TRAIN-C', '2023-11-02 09:00:00', 55.5, 238.0, 285, 710.0, 35000),
('RD-127', 'TRAIN-C', '2023-11-02 10:00:00', 55.6, 237.5, 288, 712.0, 35000),
('RD-128', 'TRAIN-C', '2023-11-02 11:00:00', 55.7, 237.0, 290, 715.0, 35000),
('RD-129', 'TRAIN-C', '2023-11-03 08:00:00', 56.0, 235.0, 300, 720.0, 35000),
('RD-130', 'TRAIN-A', '2023-11-04 08:00:00', 60.0, 200.0, 340, 800.0, 35000), -- Post CIP? Back to normal
('RD-131', 'TRAIN-A', '2023-11-04 09:00:00', 60.0, 200.0, 340, 800.0, 35000),
('RD-132', 'TRAIN-A', '2023-11-04 10:00:00', 60.0, 200.0, 340, 800.0, 35000),
('RD-133', 'TRAIN-B', '2023-11-03 09:00:00', 59.1, 204.0, 322, 782.0, 35000),
('RD-134', 'TRAIN-B', '2023-11-03 10:00:00', 59.2, 204.0, 325, 785.0, 35000),
('RD-135', 'TRAIN-C', '2023-11-03 09:00:00', 56.1, 235.0, 302, 722.0, 35000),
('RD-136', 'TRAIN-C', '2023-11-03 10:00:00', 56.2, 234.0, 305, 725.0, 35000),
('RD-137', 'TRAIN-A', '2023-11-04 11:00:00', 60.1, 199.5, 342, 802.0, 35000),
('RD-138', 'TRAIN-A', '2023-11-05 08:00:00', 60.2, 199.0, 345, 804.0, 35000),
('RD-139', 'TRAIN-B', '2023-11-04 08:00:00', 59.5, 202.0, 330, 790.0, 35000),
('RD-140', 'TRAIN-B', '2023-11-04 09:00:00', 59.6, 202.0, 332, 792.0, 35000),
('RD-141', 'TRAIN-B', '2023-11-04 10:00:00', 59.7, 201.5, 335, 795.0, 35000),
('RD-142', 'TRAIN-C', '2023-11-04 08:00:00', 56.5, 230.0, 310, 730.0, 35000),
('RD-143', 'TRAIN-C', '2023-11-04 09:00:00', 56.6, 230.0, 312, 732.0, 35000),
('RD-144', 'TRAIN-C', '2023-11-04 10:00:00', 56.7, 229.0, 315, 735.0, 35000),
('RD-145', 'TRAIN-A', '2023-11-05 09:00:00', 60.3, 199.0, 348, 806.0, 35000),
('RD-146', 'TRAIN-B', '2023-11-05 08:00:00', 60.0, 200.0, 340, 800.0, 35000),
('RD-147', 'TRAIN-B', '2023-11-05 09:00:00', 60.1, 200.0, 340, 802.0, 35000),
('RD-148', 'TRAIN-C', '2023-11-05 08:00:00', 57.0, 225.0, 320, 740.0, 35000),
('RD-149', 'TRAIN-C', '2023-11-05 09:00:00', 57.1, 225.0, 322, 742.0, 35000),
('RD-150', 'TRAIN-C', '2023-11-05 10:00:00', 57.2, 224.0, 325, 745.0, 35000);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Hydro_Ops.Bronze.Bronze_Trains AS SELECT * FROM Hydro_Ops.Sources.RO_Trains;
CREATE OR REPLACE VIEW Hydro_Ops.Bronze.Bronze_Sensors AS SELECT * FROM Hydro_Ops.Sources.Sensor_Telemetry;

-- 4b. SILVER LAYER (Specific Energy Consumption)
CREATE OR REPLACE VIEW Hydro_Ops.Silver.Silver_Train_Performance AS
SELECT
    s.TrainID,
    t.Membrane_Type,
    s.Timestamp,
    s.Feed_Pressure_Bar,
    s.Permeate_Flow_m3_Hr,
    s.Conductivity_uS_cm,
    s.Power_Consumption_kW,
    -- SEC (Specific Energy Consumption) = kW / (m3/hr) = kWh/m3
    ROUND(s.Power_Consumption_kW / NULLIF(s.Permeate_Flow_m3_Hr, 0), 2) as SEC_kWh_m3,
    -- Normalized Pressure Drop Proxy (Feed Pressure vs Flow)
    s.Feed_Pressure_Bar / NULLIF(s.Permeate_Flow_m3_Hr, 0) as Fouling_Index
FROM Hydro_Ops.Bronze.Bronze_Sensors s
JOIN Hydro_Ops.Bronze.Bronze_Trains t ON s.TrainID = t.TrainID;

-- 4c. GOLD LAYER (Cleaning Schedule Optimizer)
CREATE OR REPLACE VIEW Hydro_Ops.Gold.Gold_CIP_Advisor AS
SELECT
    TrainID,
    Membrane_Type,
    AVG(SEC_kWh_m3) as Avg_Energy_Usage,
    MAX(Fouling_Index) as Peak_Fouling,
    AVG(Conductivity_uS_cm) as Purity_Metric,
    -- Recommendation Logic
    CASE 
        WHEN MAX(Fouling_Index) > 0.35 THEN 'Urgent CIP Required' -- High pressure, low flow
        WHEN AVG(SEC_kWh_m3) > 4.0 THEN 'Schedule CIP (Energy Inefficient)'
        WHEN AVG(Conductivity_uS_cm) > 450 THEN 'Check Membrane Integrity'
        ELSE 'Optimal'
    END as Operational_Status
FROM Hydro_Ops.Silver.Silver_Train_Performance
WHERE Timestamp > CURRENT_DATE - 7 -- Last 7 days rolling
GROUP BY TrainID, Membrane_Type;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Compare 'Silver_Train_Performance' for 'SWRO-HighReject' vs 'SWRO-LowEnergy'. 
 * Calculate the cost savings per m3 if we switched all trains to LowEnergy membranes, assuming $0.12/kWh."
 */
