/*
    Dremio High-Volume SQL Pattern: Government Lobbying Registration
    
    Business Scenario:
    Ethics Commissions track "Lobbying Spend" to ensure transparency.
    Identifying top influencers and sectors help in regulatory oversight.
    
    Data Story:
    We track Lobbyist Registrations and Expenditure Reports.
    
    Medallion Architecture:
    - Bronze: Lobbyists, Clients, Expenditures.
      *Volume*: 50+ records.
    - Silver: ClientSpend (Total spend per client).
    - Gold: InfluenceAnalysis (Top sectors by spend).
    
    Key Dremio Features:
    - Summation
    - Ranking
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentLobbyingDB;
CREATE FOLDER IF NOT EXISTS GovernmentLobbyingDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentLobbyingDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentLobbyingDB.Gold;
USE GovernmentLobbyingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentLobbyingDB.Bronze.Clients (
    ClientID STRING,
    Name STRING,
    Sector STRING -- Enroll, Real Estate, Tech
);

INSERT INTO GovernmentLobbyingDB.Bronze.Clients VALUES
('C1', 'Big Oil Corp', 'Energy'),
('C2', 'Dev Group LLC', 'Real Estate'),
('C3', 'Tech Giants Assoc', 'Tech'),
('C4', 'Pharma United', 'Healthcare'),
('C5', 'AgriBusiness Inc', 'Agriculture'),
('C6', 'Transit Workers', 'Labor'),
('C7', 'EduFirst', 'Education'),
('C8', 'Bankers League', 'Finance');

CREATE OR REPLACE TABLE GovernmentLobbyingDB.Bronze.Expenditures (
    ReportID STRING,
    LobbyistID STRING,
    ClientID STRING,
    Amount DOUBLE,
    Purpose STRING, -- Dinner, Ad, Contribution
    Date DATE
);

-- Bulk Spend (50 Record)
INSERT INTO GovernmentLobbyingDB.Bronze.Expenditures VALUES
('EXP1001', 'LOB101', 'C1', 5000.0, 'Contribution', DATE '2024-11-01'),
('EXP1002', 'LOB102', 'C2', 12000.0, 'Ad', DATE '2024-11-05'),
('EXP1003', 'LOB103', 'C3', 8000.0, 'Dinner', DATE '2024-11-10'),
('EXP1004', 'LOB104', 'C4', 15000.0, 'Contribution', DATE '2024-11-15'),
('EXP1005', 'LOB101', 'C1', 2000.0, 'Dinner', DATE '2024-11-20'),
('EXP1006', 'LOB105', 'C5', 500.0, 'Dinner', DATE '2024-12-01'),
('EXP1007', 'LOB106', 'C6', 1000.0, 'Ad', DATE '2024-12-05'),
('EXP1008', 'LOB107', 'C7', 3000.0, 'Contribution', DATE '2024-12-10'),
('EXP1009', 'LOB108', 'C8', 20000.0, 'Contribution', DATE '2024-12-15'),
('EXP1010', 'LOB102', 'C2', 5000.0, 'Ad', DATE '2025-01-01'),
('EXP1011', 'LOB103', 'C3', 1200.0, 'Dinner', DATE '2025-01-05'),
('EXP1012', 'LOB104', 'C4', 6000.0, 'Ad', DATE '2025-01-10'),
('EXP1013', 'LOB101', 'C1', 10000.0, 'Contribution', DATE '2025-01-15'),
('EXP1014', 'LOB108', 'C8', 25000.0, 'Ad', DATE '2025-01-18'),
('EXP1015', 'LOB102', 'C2', 3000.0, 'Dinner', DATE '2025-01-20'),
('EXP1016', 'LOB105', 'C5', 1500.0, 'Ad', DATE '2024-11-25'),
('EXP1017', 'LOB106', 'C6', 2000.0, 'Dinner', DATE '2024-12-08'),
('EXP1018', 'LOB107', 'C7', 4000.0, 'Ad', DATE '2024-12-12'),
('EXP1019', 'LOB103', 'C3', 9000.0, 'Contribution', DATE '2024-12-20'),
('EXP1020', 'LOB104', 'C4', 7000.0, 'Dinner', DATE '2025-01-02'),
('EXP1021', 'LOB101', 'C1', 3500.0, 'Ad', DATE '2025-01-05'),
('EXP1022', 'LOB108', 'C8', 5000.0, 'Dinner', DATE '2025-01-10'),
('EXP1023', 'LOB102', 'C2', 15000.0, 'Contribution', DATE '2025-01-15'),
('EXP1024', 'LOB105', 'C5', 800.0, 'Dinner', DATE '2024-11-30'),
('EXP1025', 'LOB106', 'C6', 2500.0, 'Contribution', DATE '2024-12-15'),
('EXP1026', 'LOB107', 'C7', 1200.0, 'Dinner', DATE '2024-12-18'),
('EXP1027', 'LOB103', 'C3', 11000.0, 'Ad', DATE '2025-01-01'),
('EXP1028', 'LOB104', 'C4', 8000.0, 'Contribution', DATE '2025-01-12'),
('EXP1029', 'LOB101', 'C1', 6000.0, 'Ad', DATE '2025-01-20'),
('EXP1030', 'LOB108', 'C8', 18000.0, 'Contribution', DATE '2025-01-05'),
('EXP1031', 'LOB102', 'C2', 4000.0, 'Dinner', DATE '2025-01-08'),
('EXP1032', 'LOB105', 'C5', 1000.0, 'Ad', DATE '2024-12-01'),
('EXP1033', 'LOB106', 'C6', 500.0, 'Dinner', DATE '2024-12-05'),
('EXP1034', 'LOB107', 'C7', 2000.0, 'Ad', DATE '2024-12-25'),
('EXP1035', 'LOB103', 'C3', 5000.0, 'Contribution', DATE '2025-01-08'),
('EXP1036', 'LOB104', 'C4', 2500.0, 'Dinner', DATE '2025-01-15'),
('EXP1037', 'LOB101', 'C1', 7500.0, 'Contribution', DATE '2024-12-25'),
('EXP1038', 'LOB108', 'C8', 12000.0, 'Ad', DATE '2025-01-01'),
('EXP1039', 'LOB102', 'C2', 6500.0, 'Ad', DATE '2025-01-10'),
('EXP1040', 'LOB105', 'C5', 600.0, 'Dinner', DATE '2024-11-10'),
('EXP1041', 'LOB106', 'C6', 1500.0, 'Contribution', DATE '2024-11-15'),
('EXP1042', 'LOB107', 'C7', 3500.0, 'Ad', DATE '2024-12-01'),
('EXP1043', 'LOB103', 'C3', 9500.0, 'Ad', DATE '2024-12-20'),
('EXP1044', 'LOB104', 'C4', 4000.0, 'Ad', DATE '2025-01-05'),
('EXP1045', 'LOB101', 'C1', 1200.0, 'Dinner', DATE '2025-01-15'),
('EXP1046', 'LOB108', 'C8', 30000.0, 'Contribution', DATE '2025-01-20'), -- Big spend
('EXP1047', 'LOB102', 'C2', 2000.0, 'Dinner', DATE '2025-01-18'),
('EXP1048', 'LOB105', 'C5', 900.0, 'Ad', DATE '2024-12-10'),
('EXP1049', 'LOB106', 'C6', 800.0, 'Dinner', DATE '2025-01-01'),
('EXP1050', 'LOB107', 'C7', 1500.0, 'Contribution', DATE '2025-01-05');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Spend Aggregation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentLobbyingDB.Silver.ClientTotalSpend AS
SELECT
    c.Name,
    c.Sector,
    SUM(e.Amount) AS TotalSpend
FROM GovernmentLobbyingDB.Bronze.Expenditures e
JOIN GovernmentLobbyingDB.Bronze.Clients c ON e.ClientID = c.ClientID
GROUP BY c.Name, c.Sector;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Sector Influence
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentLobbyingDB.Gold.SectorInfluence AS
SELECT
    Sector,
    SUM(TotalSpend) AS AggregateSpend,
    COUNT(DISTINCT Name) AS ActiveClients
FROM GovernmentLobbyingDB.Silver.ClientTotalSpend
GROUP BY Sector;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Sector has the highest lobbying spend?"
    2. "Show top clients by total spend."
    3. "List all expenditures for Real Estate."
*/
