/*
    Dremio High-Volume SQL Pattern: Autonomous Vehicle Fleet Telemetry
    
    Business Scenario:
    NetApp excels at managing massive unstructured and structured datasets for AI/ML.
    In this scenario, we manage telemetry from an Autonomous Vehicle (AV) fleet to
    optimize battery usage and monitor safety incidents.
    
    Industry: Automotive / AI / Technology
    
    Data Story:
    Bronze: Raw sensor streams (High volume ingestion test).
    Silver: Enriched vehicle status and anomaly flags.
    Gold: Fleet-wide efficiency and safety dashboards.
    
    Key Dremio Features:
    - High-volume INSERT performance
    - Nested SQL logic
    - Wiki Documentation (Markdown support)
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS AutonDB;
CREATE FOLDER IF NOT EXISTS AutonDB.Fleet;
CREATE FOLDER IF NOT EXISTS AutonDB.Fleet.Bronze;
CREATE FOLDER IF NOT EXISTS AutonDB.Fleet.Silver;
CREATE FOLDER IF NOT EXISTS AutonDB.Fleet.Gold;
USE AutonDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Sensor Ingestion
-------------------------------------------------------------------------------

/*
    Markdown for Bronze Wiki:
    
    # Bronze.SensorStream
    
    **Type**: Raw Ingestion Table
    **Source**: Kafka Stream `topic-av-sensors-v1`
    **Retention**: 7 Days
    
    ## Schema
    - `VehicleID`: Unique VIN or Fleet ID
    - `Timestamp`: Event time (UTC)
    - `SpeedKmh`: Velocity in km/h
    - `BatteryPct`: State of Charge (0-100)
    - `DriveMode`: 'Manual', 'Autonomous', 'Idle'
    - `LidarObjectCount`: Number of objects detected by LIDAR
*/

CREATE OR REPLACE TABLE AutonDB.Fleet.Bronze.SensorStream (
    VehicleID VARCHAR,
    Timestamp TIMESTAMP,
    SpeedKmh DOUBLE,
    BatteryPct INT,
    DriveMode VARCHAR,
    LidarObjectCount INT,
    GpsLat DOUBLE,
    GpsLon DOUBLE
);

-- Simulating 50+ records of telemetry
INSERT INTO AutonDB.Fleet.Bronze.SensorStream VALUES
('AV-001', TIMESTAMP '2025-06-01 08:00:00', 0.0, 100, 'Idle', 0, 37.7749, -122.4194),
('AV-001', TIMESTAMP '2025-06-01 08:01:00', 15.0, 99, 'Manual', 2, 37.7750, -122.4195),
('AV-001', TIMESTAMP '2025-06-01 08:02:00', 30.5, 99, 'Autonomous', 5, 37.7752, -122.4198),
('AV-001', TIMESTAMP '2025-06-01 08:03:00', 45.2, 98, 'Autonomous', 8, 37.7755, -122.4200),
('AV-001', TIMESTAMP '2025-06-01 08:04:00', 48.0, 98, 'Autonomous', 6, 37.7760, -122.4205),
('AV-001', TIMESTAMP '2025-06-01 08:05:00', 10.0, 97, 'Autonomous', 12, 37.7765, -122.4210), -- Traffic?
('AV-001', TIMESTAMP '2025-06-01 08:06:00', 0.0, 97, 'Autonomous', 15, 37.7766, -122.4211), -- Stop
('AV-002', TIMESTAMP '2025-06-01 08:00:00', 0.0, 85, 'Idle', 0, 34.0522, -118.2437),
('AV-002', TIMESTAMP '2025-06-01 08:05:00', 25.0, 84, 'Manual', 3, 34.0525, -118.2440),
('AV-002', TIMESTAMP '2025-06-01 08:10:00', 60.0, 82, 'Autonomous', 1, 34.0530, -118.2450),
('AV-002', TIMESTAMP '2025-06-01 08:15:00', 65.5, 80, 'Autonomous', 0, 34.0540, -118.2460),
('AV-002', TIMESTAMP '2025-06-01 08:20:00', 70.0, 78, 'Autonomous', 2, 34.0550, -118.2470),
('AV-003', TIMESTAMP '2025-06-01 09:00:00', 12.0, 40, 'Manual', 4, 40.7128, -74.0060),
('AV-003', TIMESTAMP '2025-06-01 09:01:00', 15.0, 39, 'Manual', 5, 40.7130, -74.0062),
('AV-003', TIMESTAMP '2025-06-01 09:02:00', 18.0, 39, 'Manual', 5, 40.7132, -74.0064),
('AV-003', TIMESTAMP '2025-06-01 09:03:00', 0.0, 38, 'Idle', 2, 40.7134, -74.0065),
('AV-004', TIMESTAMP '2025-06-01 10:00:00', 55.0, 90, 'Autonomous', 4, 51.5074, -0.1278),
('AV-004', TIMESTAMP '2025-06-01 10:05:00', 60.0, 89, 'Autonomous', 3, 51.5080, -0.1280),
('AV-004', TIMESTAMP '2025-06-01 10:10:00', 90.0, 88, 'Autonomous', 1, 51.5090, -0.1290), -- Highway speed
('AV-004', TIMESTAMP '2025-06-01 10:15:00', 95.0, 86, 'Autonomous', 0, 51.5100, -0.1300),
('AV-005', TIMESTAMP '2025-06-01 12:00:00', 0.0, 15, 'Idle', 0, 48.8566, 2.3522), -- Low battery
('AV-005', TIMESTAMP '2025-06-01 12:01:00', 0.0, 15, 'Idle', 0, 48.8566, 2.3522),
('AV-001', TIMESTAMP '2025-06-01 08:30:00', 25.0, 95, 'Autonomous', 20, 37.7800, -122.4300), -- Crowded
('AV-001', TIMESTAMP '2025-06-01 08:31:00', 22.0, 95, 'Autonomous', 22, 37.7801, -122.4301),
('AV-001', TIMESTAMP '2025-06-01 08:32:00', 20.0, 94, 'Autonomous', 25, 37.7802, -122.4302), -- Very crowded
('AV-002', TIMESTAMP '2025-06-01 12:00:00', 0.0, 60, 'Charging', 0, 34.0600, -118.2500),
('AV-002', TIMESTAMP '2025-06-01 13:00:00', 0.0, 90, 'Charging', 0, 34.0600, -118.2500), -- Charged
('AV-006', TIMESTAMP '2025-06-01 14:00:00', 40.0, 75, 'Autonomous', 2, 35.6895, 139.6917),
('AV-006', TIMESTAMP '2025-06-01 14:01:00', 42.0, 75, 'Autonomous', 2, 35.6896, 139.6918),
('AV-006', TIMESTAMP '2025-06-01 14:02:00', 45.0, 74, 'Autonomous', 3, 35.6897, 139.6919),
('AV-006', TIMESTAMP '2025-06-01 14:03:00', 80.0, 73, 'Autonomous', 5, 35.6900, 139.6930), -- Sudden accel?
('AV-006', TIMESTAMP '2025-06-01 14:04:00', 10.0, 72, 'Manual', 5, 35.6905, 139.6940), -- Disengagement to Manual
('AV-007', TIMESTAMP '2025-06-01 15:00:00', 30.0, 50, 'Manual', 8, 55.7558, 37.6173),
('AV-007', TIMESTAMP '2025-06-01 15:05:00', 35.0, 49, 'Manual', 9, 55.7560, 37.6175),
('AV-007', TIMESTAMP '2025-06-01 15:10:00', 40.0, 48, 'Manual', 10, 55.7570, 37.6180),
('AV-008', TIMESTAMP '2025-06-01 16:00:00', 70.0, 88, 'Autonomous', 1, 52.5200, 13.4050),
('AV-008', TIMESTAMP '2025-06-01 16:05:00', 75.0, 86, 'Autonomous', 0, 52.5210, 13.4060),
('AV-008', TIMESTAMP '2025-06-01 16:10:00', 110.0, 84, 'Autonomous', 0, 52.5220, 13.4080), -- Speeding?
('AV-009', TIMESTAMP '2025-06-01 17:00:00', 15.0, 92, 'Autonomous', 50, 19.4326, -99.1332), -- Dense crowd
('AV-009', TIMESTAMP '2025-06-01 17:01:00', 10.0, 92, 'Autonomous', 55, 19.4327, -99.1333),
('AV-009', TIMESTAMP '2025-06-01 17:02:00', 5.0, 91, 'Autonomous', 60, 19.4328, -99.1334),
('AV-009', TIMESTAMP '2025-06-01 17:03:00', 0.0, 91, 'Autonomous', 65, 19.4329, -99.1335), -- Gridlock
('AV-010', TIMESTAMP '2025-06-01 08:00:00', 0.0, 100, 'Idle', 0, -33.8688, 151.2093),
('AV-010', TIMESTAMP '2025-06-01 09:00:00', 20.0, 98, 'Autonomous', 5, -33.8690, 151.2100),
('AV-010', TIMESTAMP '2025-06-01 10:00:00', 40.0, 95, 'Autonomous', 10, -33.8700, 151.2200),
('AV-010', TIMESTAMP '2025-06-01 11:00:00', 30.0, 90, 'Autonomous', 8, -33.8800, 151.2300),
('AV-010', TIMESTAMP '2025-06-01 12:00:00', 0.0, 88, 'Idle', 2, -33.8850, 151.2350),
('AV-011', TIMESTAMP '2025-06-01 20:00:00', 80.0, 45, 'Autonomous', 1, 25.2048, 55.2708),
('AV-011', TIMESTAMP '2025-06-01 20:05:00', 120.0, 42, 'Autonomous', 0, 25.2100, 55.2800), -- High speed
('AV-012', TIMESTAMP '2025-06-02 08:00:00', 25.0, 99, 'Autonomous', 4, 1.3521, 103.8198),
('AV-012', TIMESTAMP '2025-06-02 08:10:00', 0.0, 98, 'Manual', 0, 1.3530, 103.8200);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Vehicle Health & Anomaly Detection
-------------------------------------------------------------------------------

/*
    Markdown for Silver Wiki:
    
    # Silver.VehicleHealth
    
    **Type**: Enriched View
    **Business Logic**:
    - **BatteryStatus**: Categorizes charge level (Critical < 20%).
    - **SpeedCategory**: Flags High Speed (> 100 kmh).
    - **SensorLoad**: Identifies heavy computation scenarios (Lidar > 20 objects).
    
    **Downstream Consumers**:
    - Fleet Operations Center
    - Maintenance Scheduling AI
*/

CREATE OR REPLACE VIEW AutonDB.Fleet.Silver.VehicleHealth AS
SELECT 
    VehicleID,
    Timestamp,
    SpeedKmh,
    BatteryPct,
    DriveMode,
    LidarObjectCount,
    CASE 
        WHEN BatteryPct < 20 THEN 'Critical'
        WHEN BatteryPct < 50 THEN 'Low'
        ELSE 'Healthy' 
    END AS BatteryStatus,
    CASE 
        WHEN SpeedKmh > 100 THEN 'High Speed'
        WHEN SpeedKmh > 0 AND SpeedKmh <= 100 THEN 'Moving'
        ELSE 'Stopped' 
    END AS MotionStatus,
    CASE 
        WHEN LidarObjectCount > 20 THEN 'High Sensor Load'
        ELSE 'Normal' 
    END AS ComputeLoad
FROM AutonDB.Fleet.Bronze.SensorStream;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Daily Fleet Efficiency
-------------------------------------------------------------------------------

/*
    Markdown for Gold Wiki:
    
    # Gold.FleetEfficiency
    
    **Type**: Aggregated Reporting View
    **Update Frequency**: Daily
    
    ## Metrics
    - `AvgSpeed`: Operational efficiency.
    - `DisengagementCount`: Safety metric (Switches to 'Manual' from 'Autonomous' context imply disengagement, though simplified here as raw count of Manual points for demo).
    - `HighSpeedEvents`: Count of speed violations.
    - `MinBattery`: Lowest charge seen (range anxiety check).
*/

CREATE OR REPLACE VIEW AutonDB.Fleet.Gold.FleetEfficiency AS
SELECT 
    VehicleID,
    CAST(Timestamp AS DATE) AS ReportDate,
    AVG(SpeedKmh) AS AvgSpeed,
    MAX(SpeedKmh) AS MaxSpeed,
    MIN(BatteryPct) AS MinBattery,
    COUNT(CASE WHEN MotionStatus = 'High Speed' THEN 1 END) AS HighSpeedEvents,
    COUNT(CASE WHEN DriveMode = 'Manual' THEN 1 END) AS ManualModeTicks,
    COUNT(CASE WHEN ComputeLoad = 'High Sensor Load' THEN 1 END) AS HighComputeEvents
FROM AutonDB.Fleet.Silver.VehicleHealth
GROUP BY VehicleID, CAST(Timestamp AS DATE);

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS (Text-to-SQL Examples)
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat:

    1. "List vehicles with 'Critical' battery status from the Silver layer."
    2. "Show the daily MaxSpeed for AV-004 in the Gold layer."
    3. "Count how many vehicles had High Compute Events on 2025-06-01."
*/
