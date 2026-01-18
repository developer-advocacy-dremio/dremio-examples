/*
    Dremio High-Volume SQL Pattern: Government Public Housing
    
    Business Scenario:
    Housing Authorities manage thousands of units. Minimizing "Vacancy Days" (time to turn over a unit)
    is critical for housing homeless populations from the Waitlist.
    
    Data Story:
    We track Housing Units and Applicant Processing.
    
    Medallion Architecture:
    - Bronze: UnitInventory, Waitlist.
      *Volume*: 50+ records.
    - Silver: UnitStatus (Occupied, Vacant, Maintenance).
    - Gold: OccupancyReport (Vacancy rates and Turnover efficiency).
    
    Key Dremio Features:
    - TIMESTAMPDIFF for vacancy calculation
    - Case logic
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentHousingDB;
CREATE FOLDER IF NOT EXISTS GovernmentHousingDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentHousingDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentHousingDB.Gold;
USE GovernmentHousingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentHousingDB.Bronze.UnitInventory (
    UnitID STRING,
    BuildingID STRING,
    Status STRING, -- Occupied, Vacant-Ready, Vacant-Maintenance
    LastTenantMoveOutDate DATE,
    Bedrooms INT
);

-- Bulk Units (50 Records)
INSERT INTO GovernmentHousingDB.Bronze.UnitInventory VALUES
('U101', 'BLDG_A', 'Occupied', NULL, 1),
('U102', 'BLDG_A', 'Occupied', NULL, 2),
('U103', 'BLDG_A', 'Vacant-Ready', DATE '2025-01-05', 3), -- Vacant 15 days
('U104', 'BLDG_A', 'Occupied', NULL, 1),
('U105', 'BLDG_A', 'Vacant-Maintenance', DATE '2024-12-15', 2), -- Long vacancy (Maint)
('U106', 'BLDG_B', 'Occupied', NULL, 2),
('U107', 'BLDG_B', 'Occupied', NULL, 3),
('U108', 'BLDG_B', 'Occupied', NULL, 1),
('U109', 'BLDG_B', 'Vacant-Ready', DATE '2025-01-18', 2),
('U110', 'BLDG_B', 'Occupied', NULL, 2),
('U111', 'BLDG_A', 'Occupied', NULL, 1),
('U112', 'BLDG_A', 'Occupied', NULL, 2),
('U113', 'BLDG_A', 'Occupied', NULL, 3),
('U114', 'BLDG_A', 'Vacant-Ready', DATE '2025-01-10', 1),
('U115', 'BLDG_A', 'Occupied', NULL, 2),
('U116', 'BLDG_B', 'Occupied', NULL, 3),
('U117', 'BLDG_B', 'Occupied', NULL, 1),
('U118', 'BLDG_B', 'Vacant-Maintenance', DATE '2025-01-01', 2),
('U119', 'BLDG_B', 'Occupied', NULL, 2),
('U120', 'BLDG_B', 'Occupied', NULL, 1),
('U121', 'BLDG_A', 'Occupied', NULL, 2),
('U122', 'BLDG_A', 'Occupied', NULL, 3),
('U123', 'BLDG_A', 'Occupied', NULL, 1),
('U124', 'BLDG_A', 'Occupied', NULL, 2),
('U125', 'BLDG_A', 'Occupied', NULL, 2),
('U126', 'BLDG_B', 'Occupied', NULL, 3),
('U127', 'BLDG_B', 'Vacant-Ready', DATE '2025-01-15', 1),
('U128', 'BLDG_B', 'Occupied', NULL, 2),
('U129', 'BLDG_B', 'Occupied', NULL, 2),
('U130', 'BLDG_B', 'Occupied', NULL, 1),
('U131', 'BLDG_A', 'Occupied', NULL, 2),
('U132', 'BLDG_A', 'Occupied', NULL, 3),
('U133', 'BLDG_A', 'Occupied', NULL, 1),
('U134', 'BLDG_A', 'Occupied', NULL, 2),
('U135', 'BLDG_A', 'Occupied', NULL, 2),
('U136', 'BLDG_B', 'Occupied', NULL, 3),
('U137', 'BLDG_B', 'Occupied', NULL, 1),
('U138', 'BLDG_B', 'Occupied', NULL, 2),
('U139', 'BLDG_B', 'Vacant-Ready', DATE '2025-01-19', 2),
('U140', 'BLDG_B', 'Occupied', NULL, 1),
('U141', 'BLDG_A', 'Occupied', NULL, 2),
('U142', 'BLDG_A', 'Occupied', NULL, 3),
('U143', 'BLDG_A', 'Occupied', NULL, 1),
('U144', 'BLDG_A', 'Occupied', NULL, 2),
('U145', 'BLDG_A', 'Occupied', NULL, 2),
('U146', 'BLDG_B', 'Occupied', NULL, 3),
('U147', 'BLDG_B', 'Occupied', NULL, 1),
('U148', 'BLDG_B', 'Occupied', NULL, 2),
('U149', 'BLDG_B', 'Occupied', NULL, 2),
('U150', 'BLDG_B', 'Vacant-Maintenance', DATE '2024-11-20', 3);

CREATE OR REPLACE TABLE GovernmentHousingDB.Bronze.Waitlist (
    ApplicantID STRING,
    ApplicationDate DATE,
    PriorityScore INT, -- 1-100
    RequiredBedrooms INT
);

-- Bulk Waitlist (50 Records)
INSERT INTO GovernmentHousingDB.Bronze.Waitlist VALUES
('APP501', DATE '2024-06-01', 95, 2),
('APP502', DATE '2024-07-15', 80, 1),
('APP503', DATE '2024-08-01', 75, 3),
('APP504', DATE '2024-08-20', 88, 2),
('APP505', DATE '2024-09-05', 60, 1),
('APP506', DATE '2024-09-15', 92, 2),
('APP507', DATE '2024-10-01', 70, 3),
('APP508', DATE '2024-10-10', 85, 1),
('APP509', DATE '2024-11-01', 90, 2),
('APP510', DATE '2024-11-20', 55, 3),
('APP511', DATE '2024-11-25', 99, 1), -- High Priority
('APP512', DATE '2024-12-01', 82, 2),
('APP513', DATE '2024-12-05', 78, 2),
('APP514', DATE '2024-12-10', 65, 1),
('APP515', DATE '2024-12-15', 72, 3),
('APP516', DATE '2024-12-20', 89, 2),
('APP517', DATE '2025-01-02', 91, 1),
('APP518', DATE '2025-01-03', 76, 2),
('APP519', DATE '2025-01-04', 68, 3),
('APP520', DATE '2025-01-05', 85, 1),
('APP521', DATE '2025-01-06', 74, 2),
('APP522', DATE '2025-01-07', 93, 2),
('APP523', DATE '2025-01-08', 81, 3),
('APP524', DATE '2025-01-09', 66, 1),
('APP525', DATE '2025-01-10', 88, 2),
('APP526', DATE '2025-01-11', 77, 2),
('APP527', DATE '2025-01-12', 59, 1),
('APP528', DATE '2025-01-13', 94, 3),
('APP529', DATE '2025-01-14', 83, 2),
('APP530', DATE '2025-01-15', 79, 1),
('APP531', DATE '2025-01-15', 71, 2),
('APP532', DATE '2025-01-15', 87, 3),
('APP533', DATE '2025-01-16', 64, 1),
('APP534', DATE '2025-01-16', 96, 2),
('APP535', DATE '2025-01-16', 84, 2),
('APP536', DATE '2025-01-17', 73, 3),
('APP537', DATE '2025-01-17', 86, 1),
('APP538', DATE '2025-01-17', 69, 2),
('APP539', DATE '2025-01-18', 97, 2),
('APP540', DATE '2025-01-18', 58, 1),
('APP541', DATE '2025-01-18', 75, 3),
('APP542', DATE '2025-01-19', 85, 2),
('APP543', DATE '2025-01-19', 90, 1),
('APP544', DATE '2025-01-19', 62, 2),
('APP545', DATE '2025-01-20', 89, 3),
('APP546', DATE '2025-01-20', 74, 2),
('APP547', DATE '2025-01-20', 98, 1),
('APP548', DATE '2025-01-20', 67, 2),
('APP549', DATE '2025-01-20', 81, 3),
('APP550', DATE '2025-01-20', 92, 2);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Vacancy Aging
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentHousingDB.Silver.VacancyAging AS
SELECT
    UnitID,
    BuildingID,
    Bedrooms,
    Status,
    LastTenantMoveOutDate,
    TIMESTAMPDIFF(DAY, LastTenantMoveOutDate, DATE '2025-01-20') AS DaysVacant
FROM GovernmentHousingDB.Bronze.UnitInventory
WHERE Status LIKE 'Vacant%';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Operational Report
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentHousingDB.Gold.HousingOps AS
SELECT
    u.BuildingID,
    COUNT(u.UnitID) AS TotalUnits,
    SUM(CASE WHEN u.Status LIKE 'Vacant%' THEN 1 ELSE 0 END) AS VacantUnits,
    AVG(v.DaysVacant) AS AvgVacancyDays,
    -- Simple match: How many applicants waiting for this size?
    (SELECT COUNT(*) FROM GovernmentHousingDB.Bronze.Waitlist w WHERE w.RequiredBedrooms = 2) AS WaitlistCount_2BR
FROM GovernmentHousingDB.Bronze.UnitInventory u
LEFT JOIN GovernmentHousingDB.Silver.VacancyAging v ON u.UnitID = v.UnitID
GROUP BY u.BuildingID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all units vacant for more than 30 days."
    2. "Show the waitlist count by Bedroom size."
    3. "Calculate the vacancy rate for Building A."
*/
