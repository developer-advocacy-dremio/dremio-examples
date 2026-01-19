/*
    Dremio High-Volume SQL Pattern: Services Rental Car Fleet
    
    Business Scenario:
    A rental car agency wants to maximize "Fleet Utilization".
    Idle cars lose money; we need to track status (Rented, Maintenance, Idle).
    
    Data Story:
    - Bronze: FleetInventory, RentalAgreements.
    - Silver: DailyUtilization (Status per car per day).
    - Gold: FleetEfficiencyReport.
    
    Medallion Architecture:
    - Bronze: FleetInventory, RentalAgreements.
      *Volume*: 50+ records.
    - Silver: DailyUtilization.
    - Gold: FleetEfficiencyReport.
    
    Key Dremio Features:
    - COUNT / SUM
    - Date overlapping logic
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ServicesDB;
CREATE FOLDER IF NOT EXISTS ServicesDB.Bronze;
CREATE FOLDER IF NOT EXISTS ServicesDB.Silver;
CREATE FOLDER IF NOT EXISTS ServicesDB.Gold;
USE ServicesDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ServicesDB.Bronze.FleetInventory (
    CarID STRING,
    MakeModel STRING,
    Class STRING, -- Economy, SUV, Luxury
    HomeBranch STRING
);

INSERT INTO ServicesDB.Bronze.FleetInventory VALUES
('C001', 'Toyota Corolla', 'Economy', 'JFK'),
('C002', 'Honda Civic', 'Economy', 'JFK'),
('C003', 'Ford Explorer', 'SUV', 'JFK'),
('C004', 'Jeep Wrangler', 'SUV', 'MIA'),
('C005', 'BMW 3 Series', 'Luxury', 'MIA'),
('C006', 'Nissan Versa', 'Economy', 'LAX'),
('C007', 'Chevy Tahoe', 'SUV', 'LAX'),
('C008', 'Tesla Model 3', 'Luxury', 'SFO'),
('C009', 'Toyota Camry', 'Standard', 'ORD'),
('C010', 'Ford Mustang', 'Convertible', 'MIA');

CREATE OR REPLACE TABLE ServicesDB.Bronze.RentalAgreements (
    RentalID INT,
    CarID STRING,
    PickupDate DATE,
    ReturnDate DATE,
    Status STRING -- Closed, Open
);

INSERT INTO ServicesDB.Bronze.RentalAgreements VALUES
-- Car 1: Busy
(1, 'C001', DATE '2025-01-01', DATE '2025-01-05', 'Closed'),
(2, 'C001', DATE '2025-01-06', DATE '2025-01-10', 'Closed'),
(3, 'C001', DATE '2025-01-12', DATE '2025-01-15', 'Closed'),
(4, 'C001', DATE '2025-01-18', DATE '2025-01-25', 'Open'),

-- Car 2: Idle
(5, 'C002', DATE '2025-01-01', DATE '2025-01-02', 'Closed'),
-- Idle until Jan 20

-- Car 3: SUV
(6, 'C003', DATE '2025-01-05', DATE '2025-01-15', 'Closed'),
(7, 'C003', DATE '2025-01-20', DATE '2025-01-22', 'Open'),

-- Car 4: MIA
(8, 'C004', DATE '2025-01-01', DATE '2025-01-30', 'Open'), -- Long term

-- Car 5: Lux
(9, 'C005', DATE '2025-01-10', DATE '2025-01-12', 'Closed'),

-- Bulk Data (Simulating rentals)
(10, 'C006', DATE '2025-01-01', DATE '2025-01-05', 'Closed'),
(11, 'C006', DATE '2025-01-07', DATE '2025-01-10', 'Closed'),
(12, 'C006', DATE '2025-01-12', DATE '2025-01-15', 'Closed'),
(13, 'C007', DATE '2025-01-02', DATE '2025-01-08', 'Closed'),
(14, 'C007', DATE '2025-01-10', DATE '2025-01-18', 'Closed'),
(15, 'C008', DATE '2025-01-01', DATE '2025-01-03', 'Closed'),
(16, 'C008', DATE '2025-01-05', DATE '2025-01-10', 'Closed'),
(17, 'C008', DATE '2025-01-15', DATE '2025-01-20', 'Closed'),
(18, 'C009', DATE '2025-01-05', DATE '2025-01-06', 'Closed'),
(19, 'C010', DATE '2025-01-01', DATE '2025-01-10', 'Closed'),
(20, 'C010', DATE '2025-01-12', DATE '2025-01-20', 'Closed'),

(21, 'C001', DATE '2025-01-26', DATE '2025-01-30', 'Open'),
(22, 'C002', DATE '2025-01-20', DATE '2025-01-25', 'Open'),
(23, 'C003', DATE '2025-01-25', DATE '2025-01-28', 'Open'),
(24, 'C005', DATE '2025-01-15', DATE '2025-01-18', 'Closed'),
(25, 'C006', DATE '2025-01-20', DATE '2025-01-22', 'Closed'),
(26, 'C007', DATE '2025-01-20', DATE '2025-01-25', 'Open'),
(27, 'C008', DATE '2025-01-22', DATE '2025-01-24', 'Open'),
(28, 'C009', DATE '2025-01-10', DATE '2025-01-15', 'Closed'),
(29, 'C009', DATE '2025-01-20', DATE '2025-01-25', 'Open'),
(30, 'C010', DATE '2025-01-22', DATE '2025-01-25', 'Open'),

(31, 'C001', DATE '2025-02-01', DATE '2025-02-05', 'Open'),
(32, 'C002', DATE '2025-01-28', DATE '2025-02-01', 'Open'),
(33, 'C003', DATE '2025-02-01', DATE '2025-02-05', 'Open'),
(34, 'C004', DATE '2025-02-01', DATE '2025-02-28', 'Open'),
(35, 'C005', DATE '2025-01-20', DATE '2025-01-25', 'Open'),
(36, 'C006', DATE '2025-01-25', DATE '2025-01-28', 'Open'),
(37, 'C007', DATE '2025-01-28', DATE '2025-02-05', 'Open'),
(38, 'C008', DATE '2025-01-28', DATE '2025-01-30', 'Open'),
(39, 'C009', DATE '2025-01-28', DATE '2025-02-02', 'Open'),
(40, 'C010', DATE '2025-01-28', DATE '2025-02-05', 'Open'),
(41, 'C001', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(42, 'C002', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(43, 'C003', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(44, 'C005', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(45, 'C006', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(46, 'C007', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(47, 'C008', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(48, 'C009', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(49, 'C010', DATE '2025-02-10', DATE '2025-02-15', 'Open'),
(50, 'C004', DATE '2025-03-01', DATE '2025-03-05', 'Open');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Daily Utilization
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.DailyUtilization AS
SELECT
    f.CarID,
    f.Class,
    f.HomeBranch,
    '2025-01-20' AS ReportDate,
    CASE 
        WHEN r.RentalID IS NOT NULL THEN 'RENTED'
        ELSE 'IDLE'
    END AS Status
FROM ServicesDB.Bronze.FleetInventory f
LEFT JOIN ServicesDB.Bronze.RentalAgreements r 
    ON f.CarID = r.CarID 
    AND DATE '2025-01-20' BETWEEN r.PickupDate AND r.ReturnDate;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Fleet Efficiency Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.FleetEfficiencyReport AS
SELECT
    HomeBranch,
    Class,
    COUNT(*) AS TotalCars,
    SUM(CASE WHEN Status = 'RENTED' THEN 1 ELSE 0 END) AS RentedCount,
    (CAST(SUM(CASE WHEN Status = 'RENTED' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) * 100.0 AS UtilizationPct
FROM ServicesDB.Silver.DailyUtilization
GROUP BY HomeBranch, Class;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Average utilization by Car Class."
    2. "Which Branch has the most idle cars?"
    3. "List cars currently rented (as of 2025-01-20)."
*/
