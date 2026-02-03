/*
 * Dremio Skyscraper Elevator Dispatch Example
 * 
 * Domain: Smart Buildings & Vertical Transportation
 * Scenario: 
 * A 100-story skyscraper uses an "Observer" dispatch algorithm.
 * It tracks "Hall Calls" (button presses on floors) and "Car Loads" (weight/people).
 * The goal is to minimize "Average Time to Destination" (ATTD) and "Long Waits" (>60s).
 * 
 * Complexity: Medium (Time window grouping, efficiency metrics)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Building_Ops;
CREATE FOLDER IF NOT EXISTS Building_Ops.Sources;
CREATE FOLDER IF NOT EXISTS Building_Ops.Bronze;
CREATE FOLDER IF NOT EXISTS Building_Ops.Silver;
CREATE FOLDER IF NOT EXISTS Building_Ops.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Building_Ops.Sources.Elevator_Bank (
    CarID VARCHAR,
    Bank_Zone VARCHAR, -- 'Low-Rise (1-30)', 'Mid-Rise (31-60)', 'High-Rise (61-100)'
    Capacity_Kg INT,
    Service_Status VARCHAR -- 'Active', 'Maintenance', 'Fire_Mode'
);

CREATE TABLE IF NOT EXISTS Building_Ops.Sources.Dispatch_Logs (
    TripID VARCHAR,
    CarID VARCHAR,
    Call_Time TIMESTAMP,
    Pickup_Floor INT,
    Dropoff_Floor INT,
    Wait_Time_Seconds INT, -- Time from button press to door open
    Travel_Time_Seconds INT, -- Time inside car
    Passenger_Count INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Cars
INSERT INTO Building_Ops.Sources.Elevator_Bank VALUES
('CAR-L1', 'Low-Rise', 1600, 'Active'),
('CAR-L2', 'Low-Rise', 1600, 'Active'),
('CAR-L3', 'Low-Rise', 1600, 'Maintenance'),
('CAR-M1', 'Mid-Rise', 1600, 'Active'),
('CAR-M2', 'Mid-Rise', 1600, 'Active'),
('CAR-H1', 'High-Rise', 2000, 'Active'), -- Shuttle
('CAR-H2', 'High-Rise', 2000, 'Active');

-- Seed Trips (Morning Rush Hour simulation)
-- Up peak: Lobby (0) to Floors
INSERT INTO Building_Ops.Sources.Dispatch_Logs VALUES
('TRIP-001', 'CAR-L1', '2023-11-01 08:00:00', 0, 15, 5, 20, 10),
('TRIP-002', 'CAR-L1', '2023-11-01 08:02:00', 0, 25, 10, 30, 12),
('TRIP-003', 'CAR-L2', '2023-11-01 08:00:15', 0, 10, 0, 15, 8),
('TRIP-004', 'CAR-M1', '2023-11-01 08:05:00', 0, 45, 15, 45, 15),
('TRIP-005', 'CAR-M1', '2023-11-01 08:10:00', 0, 50, 20, 50, 15),
('TRIP-006', 'CAR-H1', '2023-11-01 08:05:00', 0, 90, 30, 60, 20), -- Long wait
('TRIP-007', 'CAR-H1', '2023-11-01 08:10:00', 0, 95, 45, 65, 18),
('TRIP-008', 'CAR-H2', '2023-11-01 08:12:00', 0, 80, 5, 55, 10),
-- Lunch Rush (Inter-floor + Lobby Down)
('TRIP-009', 'CAR-L1', '2023-11-01 12:00:00', 15, 0, 10, 20, 5),
('TRIP-010', 'CAR-L2', '2023-11-01 12:05:00', 20, 0, 15, 25, 8),
('TRIP-011', 'CAR-M1', '2023-11-01 12:10:00', 45, 0, 60, 40, 15), -- Bad wait!
('TRIP-012', 'CAR-M1', '2023-11-01 12:15:00', 50, 0, 70, 45, 15), -- Terrible wait
('TRIP-013', 'CAR-H1', '2023-11-01 12:10:00', 90, 0, 20, 55, 10),
('TRIP-014', 'CAR-L1', '2023-11-01 12:30:00', 0, 14, 5, 18, 5),
('TRIP-015', 'CAR-L1', '2023-11-01 12:35:00', 14, 0, 5, 18, 5),
('TRIP-016', 'CAR-M2', '2023-11-01 12:20:00', 35, 40, 10, 10, 2), -- Interfloor traffic
('TRIP-017', 'CAR-M2', '2023-11-01 12:25:00', 40, 0, 15, 35, 5),
('TRIP-018', 'CAR-H1', '2023-11-01 09:00:00', 0, 88, 10, 55, 5),
('TRIP-019', 'CAR-H1', '2023-11-01 09:10:00', 88, 0, 10, 55, 0), -- Empty return?
('TRIP-020', 'CAR-H2', '2023-11-01 09:15:00', 0, 75, 5, 50, 2),
('TRIP-021', 'CAR-L1', '2023-11-01 13:00:00', 0, 5, 5, 10, 10),
('TRIP-022', 'CAR-L1', '2023-11-01 13:05:00', 5, 0, 5, 10, 0),
('TRIP-023', 'CAR-L1', '2023-11-01 13:10:00', 0, 6, 5, 11, 2),
('TRIP-024', 'CAR-L1', '2023-11-01 13:15:00', 6, 0, 5, 11, 0),
('TRIP-025', 'CAR-M1', '2023-11-01 13:00:00', 35, 0, 20, 30, 8),
('TRIP-026', 'CAR-M1', '2023-11-01 13:10:00', 0, 35, 10, 30, 5),
('TRIP-027', 'CAR-H1', '2023-11-01 14:00:00', 70, 80, 30, 15, 3),
('TRIP-028', 'CAR-H1', '2023-11-01 14:10:00', 80, 90, 10, 15, 2),
('TRIP-029', 'CAR-H1', '2023-11-01 14:20:00', 90, 0, 10, 60, 5),
('TRIP-030', 'CAR-L2', '2023-11-01 15:00:00', 0, 10, 5, 15, 5),
('TRIP-031', 'CAR-L2', '2023-11-01 15:05:00', 10, 20, 5, 15, 4), -- Stepping up
('TRIP-032', 'CAR-L2', '2023-11-01 15:10:00', 20, 30, 5, 15, 3),
('TRIP-033', 'CAR-H2', '2023-11-01 16:00:00', 0, 100, 20, 70, 10), -- Observation deck trip
('TRIP-034', 'CAR-H2', '2023-11-01 16:15:00', 100, 0, 5, 70, 10),
('TRIP-035', 'CAR-M1', '2023-11-01 17:00:00', 40, 0, 45, 35, 12), -- Down Peak (Leaving work)
('TRIP-036', 'CAR-M1', '2023-11-01 17:05:00', 42, 0, 50, 36, 12),
('TRIP-037', 'CAR-M1', '2023-11-01 17:10:00', 45, 0, 55, 38, 12),
('TRIP-038', 'CAR-M2', '2023-11-01 17:00:00', 50, 0, 20, 40, 15),
('TRIP-039', 'CAR-M2', '2023-11-01 17:05:00', 55, 0, 25, 45, 15),
('TRIP-040', 'CAR-L1', '2023-11-01 17:00:00', 10, 0, 5, 10, 8),
('TRIP-041', 'CAR-L1', '2023-11-01 17:10:00', 15, 0, 10, 12, 8),
('TRIP-042', 'CAR-L2', '2023-11-01 17:00:00', 20, 0, 15, 15, 8),
('TRIP-043', 'CAR-H1', '2023-11-01 17:15:00', 90, 0, 60, 55, 18), -- Long wait high rise
('TRIP-044', 'CAR-H1', '2023-11-01 17:20:00', 95, 0, 65, 60, 18),
('TRIP-045', 'CAR-H2', '2023-11-01 17:30:00', 80, 0, 10, 50, 15),
('TRIP-046', 'CAR-H2', '2023-11-01 17:35:00', 85, 0, 15, 52, 15),
('TRIP-047', 'CAR-L1', '2023-11-01 18:00:00', 0, 5, 2, 8, 1), -- Late workers leaving/arriving
('TRIP-048', 'CAR-M1', '2023-11-01 18:00:00', 0, 35, 5, 25, 1),
('TRIP-049', 'CAR-H1', '2023-11-01 18:00:00', 0, 100, 10, 65, 2), -- Cleaning crew
('TRIP-050', 'CAR-H1', '2023-11-01 19:00:00', 100, 0, 5, 65, 2);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Building_Ops.Bronze.Bronze_Bank AS SELECT * FROM Building_Ops.Sources.Elevator_Bank;
CREATE OR REPLACE VIEW Building_Ops.Bronze.Bronze_Logs AS SELECT * FROM Building_Ops.Sources.Dispatch_Logs;

-- 4b. SILVER LAYER (Performance Metrics)
CREATE OR REPLACE VIEW Building_Ops.Silver.Silver_Elevator_Efficiency AS
SELECT
    l.TripID,
    l.CarID,
    b.Bank_Zone,
    l.Call_Time,
    l.Wait_Time_Seconds,
    l.Travel_Time_Seconds,
    (l.Wait_Time_Seconds + l.Travel_Time_Seconds) as Total_Trip_Time,
    -- Satisfaction Score
    CASE 
        WHEN l.Wait_Time_Seconds > 60 THEN 'Frustrated'
        WHEN l.Wait_Time_Seconds > 30 THEN 'Annoyed'
        ELSE 'Satisfied'
    END as User_Sentiment_Proxy,
    l.Passenger_Count
FROM Building_Ops.Bronze.Bronze_Logs l
JOIN Building_Ops.Bronze.Bronze_Bank b ON l.CarID = b.CarID;

-- 4c. GOLD LAYER (Hourly Zone Performance)
CREATE OR REPLACE VIEW Building_Ops.Gold.Gold_Hourly_Zone_Stats AS
SELECT
    Bank_Zone,
    TRUNC(Call_Time, 'HOUR') as Time_Hour,
    AVG(Wait_Time_Seconds) as Avg_Wait_Sec,
    MAX(Wait_Time_Seconds) as Max_Wait_Sec,
    SUM(Passenger_Count) as Total_Moved,
    SUM(CASE WHEN User_Sentiment_Proxy = 'Frustrated' THEN 1 ELSE 0 END) as Complaints_Forecast
FROM Building_Ops.Silver.Silver_Elevator_Efficiency
GROUP BY Bank_Zone, TRUNC(Call_Time, 'HOUR')
ORDER BY Time_Hour, Bank_Zone;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Gold_Hourly_Zone_Stats' to find the peak congestion hour for 'Mid-Rise'. 
 * Recommend simulating an additional car assignment from Low-Rise to Mid-Rise during that window."
 */
