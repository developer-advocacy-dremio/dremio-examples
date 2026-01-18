/*
    Dremio High-Volume SQL Pattern: Government Homeless Services
    
    Business Scenario:
    Social Services tracking "Shelter Capacity" and "Street Outreach" effectiveness.
    The goal is to move individuals from street -> shelter -> permanent housing.
    
    Data Story:
    We track Shelters, Beds, and Client Intakes.
    
    Medallion Architecture:
    - Bronze: Shelters, ClientIntakes.
      *Volume*: 50+ records.
    - Silver: BedAvailability (Capacity - Occupied).
    - Gold: SystemThroughput (Exits to Housing).
    
    Key Dremio Features:
    - Count Distinct
    - Filtered Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentHomelessDB;
CREATE FOLDER IF NOT EXISTS GovernmentHomelessDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentHomelessDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentHomelessDB.Gold;
USE GovernmentHomelessDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentHomelessDB.Bronze.Shelters (
    ShelterID STRING,
    Name STRING,
    Type STRING, -- Men, Women, Family
    TotalBeds INT
);

INSERT INTO GovernmentHomelessDB.Bronze.Shelters VALUES
('S1', 'Main St Mission', 'Men', 100),
('S2', 'Safe Haven', 'Women', 50),
('S3', 'Family First', 'Family', 20),
('S4', 'Veterans Lodge', 'Men', 30),
('S5', 'Youth Hope', 'Mixed', 25);

CREATE OR REPLACE TABLE GovernmentHomelessDB.Bronze.ClientIntakes (
    IntakeID STRING,
    ClientID STRING,
    ShelterID STRING,
    CheckInDate DATE,
    CheckOutDate DATE, -- NULL if still present
    ExitDestination STRING -- Permanent Housing, Street, Other
);

-- Bulk Intakes (50 Records)
INSERT INTO GovernmentHomelessDB.Bronze.ClientIntakes VALUES
('INT1001', 'C101', 'S1', DATE '2025-01-01', NULL, NULL),
('INT1002', 'C102', 'S1', DATE '2025-01-02', DATE '2025-01-05', 'Street'),
('INT1003', 'C103', 'S2', DATE '2025-01-03', NULL, NULL),
('INT1004', 'C104', 'S2', DATE '2025-01-03', DATE '2025-01-10', 'Permanent Housing'), -- Success
('INT1005', 'C105', 'S3', DATE '2025-01-04', NULL, NULL),
('INT1006', 'C106', 'S3', DATE '2025-01-04', NULL, NULL),
('INT1007', 'C107', 'S1', DATE '2025-01-05', NULL, NULL),
('INT1008', 'C108', 'S4', DATE '2025-01-05', NULL, NULL),
('INT1009', 'C109', 'S5', DATE '2025-01-06', DATE '2025-01-08', 'Other'),
('INT1010', 'C110', 'S1', DATE '2025-01-06', NULL, NULL),
('INT1011', 'C111', 'S2', DATE '2025-01-07', NULL, NULL),
('INT1012', 'C112', 'S3', DATE '2025-01-07', NULL, NULL),
('INT1013', 'C113', 'S1', DATE '2025-01-08', DATE '2025-01-20', 'Permanent Housing'),
('INT1014', 'C114', 'S4', DATE '2025-01-08', NULL, NULL),
('INT1015', 'C115', 'S5', DATE '2025-01-09', NULL, NULL),
('INT1016', 'C116', 'S2', DATE '2025-01-09', NULL, NULL),
('INT1017', 'C117', 'S3', DATE '2025-01-10', NULL, NULL),
('INT1018', 'C118', 'S1', DATE '2025-01-10', DATE '2025-01-11', 'Street'),
('INT1019', 'C119', 'S1', DATE '2025-01-11', NULL, NULL),
('INT1020', 'C120', 'S2', DATE '2025-01-11', NULL, NULL),
('INT1021', 'C121', 'S4', DATE '2025-01-12', NULL, NULL),
('INT1022', 'C122', 'S5', DATE '2025-01-12', DATE '2025-01-15', 'Permanent Housing'),
('INT1023', 'C123', 'S3', DATE '2025-01-13', NULL, NULL),
('INT1024', 'C124', 'S1', DATE '2025-01-13', NULL, NULL),
('INT1025', 'C125', 'S1', DATE '2025-01-14', NULL, NULL),
('INT1026', 'C126', 'S2', DATE '2025-01-14', DATE '2025-01-18', 'Other'),
('INT1027', 'C127', 'S3', DATE '2025-01-15', NULL, NULL),
('INT1028', 'C128', 'S4', DATE '2025-01-15', NULL, NULL),
('INT1029', 'C129', 'S5', DATE '2025-01-16', NULL, NULL),
('INT1030', 'C130', 'S1', DATE '2025-01-16', NULL, NULL),
('INT1031', 'C131', 'S2', DATE '2025-01-16', NULL, NULL),
('INT1032', 'C132', 'S1', DATE '2025-01-17', NULL, NULL),
('INT1033', 'C133', 'S1', DATE '2025-01-17', DATE '2025-01-19', 'Street'),
('INT1034', 'C134', 'S3', DATE '2025-01-18', NULL, NULL),
('INT1035', 'C135', 'S4', DATE '2025-01-18', NULL, NULL),
('INT1036', 'C136', 'S2', DATE '2025-01-18', NULL, NULL),
('INT1037', 'C137', 'S5', DATE '2025-01-19', NULL, NULL),
('INT1038', 'C138', 'S1', DATE '2025-01-19', NULL, NULL),
('INT1039', 'C139', 'S2', DATE '2025-01-19', DATE '2025-01-20', 'Permanent Housing'),
('INT1040', 'C140', 'S3', DATE '2025-01-20', NULL, NULL),
('INT1041', 'C141', 'S1', DATE '2025-01-20', NULL, NULL),
('INT1042', 'C142', 'S4', DATE '2025-01-20', NULL, NULL),
('INT1043', 'C143', 'S5', DATE '2025-01-20', NULL, NULL),
('INT1044', 'C144', 'S1', DATE '2025-01-20', NULL, NULL),
('INT1045', 'C145', 'S2', DATE '2025-01-20', NULL, NULL),
('INT1046', 'C146', 'S3', DATE '2025-01-20', NULL, NULL),
('INT1047', 'C147', 'S1', DATE '2025-01-20', NULL, NULL),
('INT1048', 'C148', 'S2', DATE '2025-01-20', NULL, NULL),
('INT1049', 'C149', 'S4', DATE '2025-01-20', NULL, NULL),
('INT1050', 'C150', 'S5', DATE '2025-01-20', NULL, NULL);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: nightly Census
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentHomelessDB.Silver.NightlyCensus AS
SELECT
    s.ShelterID,
    s.Name,
    s.TotalBeds,
    COUNT(i.IntakeID) AS BedsOccupied,
    (s.TotalBeds - COUNT(i.IntakeID)) AS BedsAvailable
FROM GovernmentHomelessDB.Bronze.Shelters s
LEFT JOIN GovernmentHomelessDB.Bronze.ClientIntakes i 
    ON s.ShelterID = i.ShelterID AND i.CheckOutDate IS NULL -- Currently present
GROUP BY s.ShelterID, s.Name, s.TotalBeds;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Housing Outcomes
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentHomelessDB.Gold.OutcomeMetrics AS
SELECT
    s.Name,
    COUNT(i.IntakeID) AS TotalExits,
    SUM(CASE WHEN i.ExitDestination = 'Permanent Housing' THEN 1 ELSE 0 END) AS SuccessfulPlacements,
    (CAST(SUM(CASE WHEN i.ExitDestination = 'Permanent Housing' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(i.IntakeID), 0)) * 100 AS SuccessRate
FROM GovernmentHomelessDB.Bronze.Shelters s
JOIN GovernmentHomelessDB.Bronze.ClientIntakes i ON s.ShelterID = i.ShelterID
WHERE i.CheckOutDate IS NOT NULL
GROUP BY s.Name;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show bed availability by Shelter Name."
    2. "Calculate the success rate of placing clients into Permanent Housing."
    3. "Which shelter had the most intakes this month?"
*/
