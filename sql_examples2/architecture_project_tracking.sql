/*
 * Professional Services: Architecture Firm Project Tracking
 * 
 * Scenario:
 * Tracking project profitability, employee billable hours, and milestone completion for an architecture firm.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ArchitectureDB;
CREATE FOLDER IF NOT EXISTS ArchitectureDB.Operations;
CREATE FOLDER IF NOT EXISTS ArchitectureDB.Operations.Bronze;
CREATE FOLDER IF NOT EXISTS ArchitectureDB.Operations.Silver;
CREATE FOLDER IF NOT EXISTS ArchitectureDB.Operations.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------

-- Projects Table: List of active architectural projects
CREATE TABLE IF NOT EXISTS ArchitectureDB.Operations.Bronze.Projects (
    ProjectID INT,
    ProjectName VARCHAR,
    ClientName VARCHAR,
    StartDate DATE,
    Budget DOUBLE,
    Status VARCHAR
);

INSERT INTO ArchitectureDB.Operations.Bronze.Projects VALUES
(1, 'Skyline Tower', 'Urban Corp', '2025-01-10', 5000000.00, 'Active'),
(2, 'Green Park Pavilion', 'City Parks Dept', '2025-02-01', 250000.00, 'Active'),
(3, 'Riverside Condos', 'Luxury Living LLC', '2025-01-15', 1200000.00, 'On Hold'),
(4, 'Tech Hub Campus', 'InnoTech', '2025-03-01', 3000000.00, 'Planning'),
(5, 'Main St Renovation', 'Historic Trust', '2024-11-01', 150000.00, 'Completed'),
(6, 'Seaside Resort', 'Ocean View Inc', '2025-02-15', 4500000.00, 'Active'),
(7, 'Metro Station Upgrade', 'City Transit', '2024-12-01', 800000.00, 'Active'),
(8, 'Library Annex', 'Public Library', '2025-01-20', 300000.00, 'Planning'),
(9, 'Convention Center', 'State Expo Authority', '2025-04-01', 10000000.00, 'Planning'),
(10, 'Eco Village', 'Sustainable Future', '2025-02-10', 600000.00, 'Active'),
(11, 'Highrise A', 'Developer A', '2025-01-01', 2000000.00, 'Active'),
(12, 'Highrise B', 'Developer B', '2025-01-01', 2200000.00, 'Active'),
(13, 'Mall Expansion', 'Retail Giant', '2025-03-15', 1500000.00, 'Planning'),
(14, 'Opera House', 'Arts Foundation', '2025-01-05', 3500000.00, 'Active'),
(15, 'University Lab', 'State Univ', '2024-10-01', 900000.00, 'Completed'),
(16, 'Bridge Decor', 'City Dept', '2025-02-20', 100000.00, 'Active'),
(17, 'Airport Lounge', 'Air Travel Co', '2025-01-25', 400000.00, 'Active'),
(18, 'Hotel Lobby', 'Hotel Chain', '2025-03-05', 200000.00, 'Active'),
(19, 'Suburban Homes', 'Residential Dev', '2025-01-10', 5000000.00, 'Active'),
(20, 'Data Center Facade', 'Tech Corp', '2025-02-01', 300000.00, 'Active'),
(21, 'Museum Wing', 'History Museum', '2025-01-15', 1800000.00, 'Active'),
(22, 'Sport Complex', 'Athletics Inc', '2025-04-10', 7000000.00, 'Planning'),
(23, 'Theme Park Gate', 'Fun World', '2025-02-25', 250000.00, 'Active'),
(24, 'Observatory', 'Space Inst', '2024-11-15', 1200000.00, 'Completed'),
(25, 'Botanical Garden', 'City Parks', '2025-03-01', 400000.00, 'Active'),
(26, 'Fire Station 5', 'City Fire Dept', '2025-01-05', 850000.00, 'Active'),
(27, 'Police HQ', 'City Police', '2025-02-10', 2500000.00, 'Active'),
(28, 'Courthouse Retrofit', 'County', '2024-09-01', 600000.00, 'Completed'),
(29, 'School Gym', 'School District', '2025-01-20', 550000.00, 'Active'),
(30, 'Private Villa', 'Individual', '2025-03-10', 1500000.00, 'Planning'),
(31, 'Corporate HQ', 'Big Biz', '2025-01-01', 8000000.00, 'Active'),
(32, 'Startuplab', 'Incubator', '2025-02-15', 300000.00, 'Active'),
(33, 'Artist Loft', 'Creative Co', '2025-01-12', 450000.00, 'Active'),
(34, 'Clinic', 'Health Net', '2025-03-20', 700000.00, 'Planning'),
(35, 'Veterinary Hospital', 'Pet Care', '2025-01-30', 650000.00, 'Active'),
(36, 'Bus Terminal', 'Transit Auth', '2024-12-15', 1100000.00, 'Active'),
(37, 'Ferry Dock', 'Port Auth', '2025-02-05', 900000.00, 'Active'),
(38, 'Lighthouse Resto', 'Preservation Soc', '2025-01-08', 350000.00, 'Active'),
(39, 'Beach Club', 'Resorts Int', '2025-03-25', 2000000.00, 'Planning'),
(40, 'Ski Lodge', 'Mountain Inc', '2024-10-10', 1500000.00, 'Completed'),
(41, 'Golf Clubhouse', 'Golf Co', '2025-01-18', 800000.00, 'Active'),
(42, 'Tennis Center', 'Sports Club', '2025-02-22', 500000.00, 'Active'),
(43, 'Music Hall', 'Symphony', '2025-01-28', 4000000.00, 'Active'),
(44, 'Cinema Complex', 'Movies Inc', '2025-04-05', 2500000.00, 'Planning'),
(45, 'Recording Studio', 'Music Label', '2025-02-12', 600000.00, 'Active'),
(46, 'Dance Academy', 'Dance Co', '2025-01-15', 400000.00, 'Active'),
(47, 'Yoga Retreat', 'Wellness', '2025-03-08', 300000.00, 'Active'),
(48, 'Spa Center', 'Luxury Spa', '2024-11-20', 500000.00, 'Completed'),
(49, 'Gym Chain', 'Fitness Co', '2025-01-22', 750000.00, 'Active'),
(50, 'Pool Complex', 'City Rec', '2025-02-18', 1200000.00, 'Active');

-- Timesheets Table: Hours logged by architects and engineers
CREATE TABLE IF NOT EXISTS ArchitectureDB.Operations.Bronze.Timesheets (
    EntryID INT,
    ProjectID INT,
    EmployeeID INT,
    EmployeeName VARCHAR,
    Role VARCHAR,
    HoursLogged DOUBLE,
    BillableRate DOUBLE,
    DateLogged DATE
);

INSERT INTO ArchitectureDB.Operations.Bronze.Timesheets VALUES
(1001, 1, 101, 'Alice Architect', 'Senior Architect', 8.0, 200.00, '2025-01-11'),
(1002, 1, 102, 'Bob Builder', 'Structural Engineer', 6.5, 180.00, '2025-01-11'),
(1003, 1, 103, 'Charlie Cad', 'Drafter', 7.0, 100.00, '2025-01-11'),
(1004, 2, 101, 'Alice Architect', 'Senior Architect', 2.0, 200.00, '2025-02-02'),
(1005, 6, 104, 'Diana Design', 'Interior Designer', 5.0, 150.00, '2025-02-16'),
(1006, 7, 102, 'Bob Builder', 'Structural Engineer', 8.0, 180.00, '2024-12-05'),
(1007, 10, 103, 'Charlie Cad', 'Drafter', 4.0, 100.00, '2025-02-11'),
(1008, 1, 101, 'Alice Architect', 'Senior Architect', 8.0, 200.00, '2025-01-12'),
(1009, 2, 103, 'Charlie Cad', 'Drafter', 6.0, 100.00, '2025-02-03'),
(1010, 6, 104, 'Diana Design', 'Interior Designer', 8.0, 150.00, '2025-02-17'),
(1011, 11, 105, 'Evan Elec', 'Electrical Engineer', 5.0, 170.00, '2025-01-05'),
(1012, 11, 101, 'Alice Architect', 'Senior Architect', 3.0, 200.00, '2025-01-05'),
(1013, 12, 106, 'Fiona Fire', 'Safety Consultant', 4.0, 190.00, '2025-01-06'),
(1014, 14, 107, 'Greg Glass', 'Facade Specialist', 8.0, 220.00, '2025-01-08'),
(1015, 1, 102, 'Bob Builder', 'Structural Engineer', 7.0, 180.00, '2025-01-13'),
(1016, 3, 101, 'Alice Architect', 'Senior Architect', 1.0, 200.00, '2025-01-16'),
(1017, 16, 103, 'Charlie Cad', 'Drafter', 5.5, 100.00, '2025-02-21'),
(1018, 17, 104, 'Diana Design', 'Interior Designer', 6.0, 150.00, '2025-01-26'),
(1019, 19, 101, 'Alice Architect', 'Senior Architect', 8.0, 200.00, '2025-01-15'),
(1020, 20, 107, 'Greg Glass', 'Facade Specialist', 7.0, 220.00, '2025-02-02'),
(1021, 21, 102, 'Bob Builder', 'Structural Engineer', 4.0, 180.00, '2025-01-18'),
(1022, 1, 103, 'Charlie Cad', 'Drafter', 8.0, 100.00, '2025-01-14'),
(1023, 23, 105, 'Evan Elec', 'Electrical Engineer', 6.0, 170.00, '2025-02-26'),
(1024, 25, 108, 'Hank Land', 'Landscape Arch', 8.0, 160.00, '2025-03-02'),
(1025, 26, 106, 'Fiona Fire', 'Safety Consultant', 3.0, 190.00, '2025-01-06'),
(1026, 27, 101, 'Alice Architect', 'Senior Architect', 5.0, 200.00, '2025-02-12'),
(1027, 29, 102, 'Bob Builder', 'Structural Engineer', 6.0, 180.00, '2025-01-21'),
(1028, 31, 107, 'Greg Glass', 'Facade Specialist', 8.0, 220.00, '2025-01-03'),
(1029, 32, 104, 'Diana Design', 'Interior Designer', 4.0, 150.00, '2025-02-16'),
(1030, 33, 101, 'Alice Architect', 'Senior Architect', 2.0, 200.00, '2025-01-13'),
(1031, 35, 105, 'Evan Elec', 'Electrical Engineer', 5.0, 170.00, '2025-02-01'),
(1032, 36, 102, 'Bob Builder', 'Structural Engineer', 8.0, 180.00, '2024-12-16'),
(1033, 37, 103, 'Charlie Cad', 'Drafter', 7.5, 100.00, '2025-02-06'),
(1034, 38, 101, 'Alice Architect', 'Senior Architect', 4.0, 200.00, '2025-01-09'),
(1035, 41, 108, 'Hank Land', 'Landscape Arch', 6.0, 160.00, '2025-01-19'),
(1036, 42, 101, 'Alice Architect', 'Senior Architect', 5.0, 200.00, '2025-02-23'),
(1037, 43, 106, 'Fiona Fire', 'Safety Consultant', 2.0, 190.00, '2025-01-29'),
(1038, 45, 105, 'Evan Elec', 'Electrical Engineer', 8.0, 170.00, '2025-02-13'),
(1039, 46, 104, 'Diana Design', 'Interior Designer', 7.0, 150.00, '2025-01-16'),
(1040, 47, 108, 'Hank Land', 'Landscape Arch', 4.0, 160.00, '2025-03-09'),
(1041, 49, 102, 'Bob Builder', 'Structural Engineer', 6.0, 180.00, '2025-01-23'),
(1042, 50, 103, 'Charlie Cad', 'Drafter', 8.0, 100.00, '2025-02-20'),
(1043, 1, 101, 'Alice Architect', 'Senior Architect', 8.0, 200.00, '2025-01-16'),
(1044, 2, 103, 'Charlie Cad', 'Drafter', 5.0, 100.00, '2025-02-05'),
(1045, 6, 105, 'Evan Elec', 'Electrical Engineer', 4.0, 170.00, '2025-02-18'),
(1046, 7, 106, 'Fiona Fire', 'Safety Consultant', 3.0, 190.00, '2024-12-06'),
(1047, 11, 102, 'Bob Builder', 'Structural Engineer', 7.0, 180.00, '2025-01-06'),
(1048, 19, 107, 'Greg Glass', 'Facade Specialist', 6.0, 220.00, '2025-01-16'),
(1049, 21, 104, 'Diana Design', 'Interior Designer', 5.0, 150.00, '2025-01-19'),
(1050, 31, 101, 'Alice Architect', 'Senior Architect', 8.0, 200.00, '2025-01-04');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Project Billing Agreggation
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ArchitectureDB.Operations.Silver.ProjectBilling AS
SELECT 
    p.ProjectID,
    p.ProjectName,
    p.ClientName,
    p.Status,
    t.EmployeeName,
    t.Role,
    t.HoursLogged,
    t.BillableRate,
    (t.HoursLogged * t.BillableRate) AS TotalBilledAmount,
    t.DateLogged
FROM ArchitectureDB.Operations.Bronze.Projects p
JOIN ArchitectureDB.Operations.Bronze.Timesheets t ON p.ProjectID = t.ProjectID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Profitability & Utilization
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ArchitectureDB.Operations.Gold.ProjectProfitability AS
SELECT 
    ProjectName,
    ClientName,
    Status,
    SUM(HoursLogged) AS TotalHours,
    SUM(TotalBilledAmount) AS TotalRevenue,
    COUNT(DISTINCT EmployeeName) AS TeamSize
FROM ArchitectureDB.Operations.Silver.ProjectBilling
GROUP BY ProjectName, ClientName, Status;

CREATE OR REPLACE VIEW ArchitectureDB.Operations.Gold.EmployeeUtilization AS
SELECT
    EmployeeName,
    Role,
    SUM(HoursLogged) AS TotalHoursLogged,
    SUM(TotalBilledAmount) AS RevenueGenerated,
    AVG(BillableRate) AS AvgRate
FROM ArchitectureDB.Operations.Silver.ProjectBilling
GROUP BY EmployeeName, Role;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Using ArchitectureDB.Operations.Gold.ProjectProfitability, list the top 5 active projects by total revenue generated."

PROMPT 2:
"Calculate the total billable hours and revenue for 'Alice Architect' using the EmployeeUtilization view."

PROMPT 3:
"Show me which projects are over budget based on the ProjectProfitability view (assuming budget is tracked elsewhere or comparing revenue to an estimated cost)."
*/
