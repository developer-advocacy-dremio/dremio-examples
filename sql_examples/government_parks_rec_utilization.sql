/*
    Dremio High-Volume SQL Pattern: Government Parks & Rec Utilization
    
    Business Scenario:
    Parks Departments generate revenue by renting facilities (Soccer fields, Picnic shelters).
    Understanding "Utilization Rate" helps set pricing and maintenance schedules.
    
    Data Story:
    We track Facility Master data and Reservation Logs.
    
    Medallion Architecture:
    - Bronze: Facilities, Reservations.
      *Volume*: 50+ records.
    - Silver: DailyUsage (Hours booked vs Available).
    - Gold: RevenueAnalysis (Revenue per Park).
    
    Key Dremio Features:
    - Date Aggregation
    - Revenue Summation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentParksDB;
CREATE FOLDER IF NOT EXISTS GovernmentParksDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentParksDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentParksDB.Gold;
USE GovernmentParksDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentParksDB.Bronze.Facilities (
    FacilityID STRING,
    ParkName STRING,
    Type STRING, -- Field, Shelter, Pool
    HourlyRate DOUBLE
);

INSERT INTO GovernmentParksDB.Bronze.Facilities VALUES
('F1', 'Central Park', 'Field', 50.0),
('F2', 'Central Park', 'Shelter', 30.0),
('F3', 'Riverside Park', 'Field', 40.0),
('F4', 'Riverside Park', 'Pool', 100.0);

CREATE OR REPLACE TABLE GovernmentParksDB.Bronze.Reservations (
    ReservationID STRING,
    FacilityID STRING,
    Date DATE,
    HoursBooked INT,
    Status STRING -- Confirmed, Cancelled
);

-- Bulk Reservations (50 Records)
INSERT INTO GovernmentParksDB.Bronze.Reservations VALUES
('R1001', 'F1', DATE '2025-05-01', 4, 'Confirmed'),
('R1002', 'F2', DATE '2025-05-01', 2, 'Confirmed'),
('R1003', 'F3', DATE '2025-05-01', 4, 'Confirmed'),
('R1004', 'F4', DATE '2025-05-01', 2, 'Confirmed'),
('R1005', 'F1', DATE '2025-05-02', 4, 'Cancelled'),
('R1006', 'F2', DATE '2025-05-02', 2, 'Confirmed'),
('R1007', 'F3', DATE '2025-05-02', 4, 'Confirmed'),
('R1008', 'F4', DATE '2025-05-02', 2, 'Confirmed'),
('R1009', 'F1', DATE '2025-05-03', 4, 'Confirmed'),
('R1010', 'F2', DATE '2025-05-03', 2, 'Confirmed'),
('R1011', 'F3', DATE '2025-05-03', 4, 'Confirmed'),
('R1012', 'F4', DATE '2025-05-03', 2, 'Cancelled'),
('R1013', 'F1', DATE '2025-05-04', 4, 'Confirmed'),
('R1014', 'F2', DATE '2025-05-04', 2, 'Confirmed'),
('R1015', 'F3', DATE '2025-05-04', 4, 'Confirmed'),
('R1016', 'F4', DATE '2025-05-04', 2, 'Confirmed'),
('R1017', 'F1', DATE '2025-05-05', 4, 'Confirmed'),
('R1018', 'F2', DATE '2025-05-05', 2, 'Confirmed'),
('R1019', 'F3', DATE '2025-05-05', 4, 'Confirmed'),
('R1020', 'F4', DATE '2025-05-05', 2, 'Confirmed'),
('R1021', 'F1', DATE '2025-05-06', 4, 'Confirmed'),
('R1022', 'F2', DATE '2025-05-06', 2, 'Confirmed'),
('R1023', 'F3', DATE '2025-05-06', 4, 'Confirmed'),
('R1024', 'F4', DATE '2025-05-06', 2, 'Confirmed'),
('R1025', 'F1', DATE '2025-05-07', 4, 'Confirmed'),
('R1026', 'F2', DATE '2025-05-07', 2, 'Confirmed'),
('R1027', 'F3', DATE '2025-05-07', 4, 'Confirmed'),
('R1028', 'F4', DATE '2025-05-07', 2, 'Confirmed'),
('R1029', 'F1', DATE '2025-05-08', 4, 'Confirmed'),
('R1030', 'F2', DATE '2025-05-08', 2, 'Confirmed'),
('R1031', 'F3', DATE '2025-05-08', 4, 'Cancelled'),
('R1032', 'F4', DATE '2025-05-08', 2, 'Confirmed'),
('R1033', 'F1', DATE '2025-05-09', 4, 'Confirmed'),
('R1034', 'F2', DATE '2025-05-09', 2, 'Confirmed'),
('R1035', 'F3', DATE '2025-05-09', 4, 'Confirmed'),
('R1036', 'F4', DATE '2025-05-09', 2, 'Confirmed'),
('R1037', 'F1', DATE '2025-05-10', 4, 'Confirmed'),
('R1038', 'F2', DATE '2025-05-10', 2, 'Confirmed'),
('R1039', 'F3', DATE '2025-05-10', 4, 'Confirmed'),
('R1040', 'F4', DATE '2025-05-10', 2, 'Confirmed'),
('R1041', 'F1', DATE '2025-05-11', 4, 'Confirmed'),
('R1042', 'F2', DATE '2025-05-11', 2, 'Confirmed'),
('R1043', 'F3', DATE '2025-05-11', 4, 'Confirmed'),
('R1044', 'F4', DATE '2025-05-11', 2, 'Confirmed'),
('R1045', 'F1', DATE '2025-05-12', 4, 'Confirmed'),
('R1046', 'F2', DATE '2025-05-12', 2, 'Confirmed'),
('R1047', 'F3', DATE '2025-05-12', 4, 'Confirmed'),
('R1048', 'F4', DATE '2025-05-12', 2, 'Confirmed'),
('R1049', 'F1', DATE '2025-05-13', 4, 'Confirmed'),
('R1050', 'F2', DATE '2025-05-13', 2, 'Confirmed');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Usage & Cost
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentParksDB.Silver.EnrichedBookings AS
SELECT
    r.ReservationID,
    r.Date,
    f.ParkName,
    f.Type,
    r.HoursBooked,
    (r.HoursBooked * f.HourlyRate) AS TotalRevenue,
    r.Status
FROM GovernmentParksDB.Bronze.Reservations r
JOIN GovernmentParksDB.Bronze.Facilities f ON r.FacilityID = f.FacilityID
WHERE r.Status = 'Confirmed';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Revenue Dashboard
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentParksDB.Gold.ParkRevenue AS
SELECT
    ParkName,
    Type,
    SUM(HoursBooked) AS TotalHoursRented,
    SUM(TotalRevenue) AS ActualRevenue,
    -- Projected vs Actual could be added here
    AVG(TotalRevenue) AS AvgBookingValue
FROM GovernmentParksDB.Silver.EnrichedBookings
GROUP BY ParkName, Type;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Park generated the most revenue?"
    2. "Show usage hours for 'Field' type facilities."
    3. "List all cancelled reservations."
*/
