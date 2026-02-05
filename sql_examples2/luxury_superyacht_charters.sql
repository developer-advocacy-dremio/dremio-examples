/*
 * Dremio Luxury Superyacht Charters Example
 * 
 * Domain: Hospitality & Maritime
 * Scenario: 
 * Superyacht management firms handle "Preference Sheets" (Guest desires).
 * They track "Fuel Bunkering" (Tens of thousands of liters), "Port Fees", and "Provisions".
 * The goal is to ensure the "Guest Experience Score" is perfect.
 * 
 * Complexity: Medium (Cost aggregation, qualitative preference matching)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Yacht_Life;
CREATE FOLDER IF NOT EXISTS Yacht_Life.Sources;
CREATE FOLDER IF NOT EXISTS Yacht_Life.Bronze;
CREATE FOLDER IF NOT EXISTS Yacht_Life.Silver;
CREATE FOLDER IF NOT EXISTS Yacht_Life.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Yacht_Life.Sources.Yacht_Registry (
    YachtID VARCHAR,
    Yacht_Name VARCHAR, -- 'S.S. Opulence'
    Length_Meters INT,
    Weekly_Rate_USD INT
);

CREATE TABLE IF NOT EXISTS Yacht_Life.Sources.Charter_Log (
    CharterID VARCHAR,
    YachtID VARCHAR,
    Start_Date DATE,
    Port_Location VARCHAR, -- 'Monaco', 'St. Tropez'
    Fuel_Added_Liters INT,
    Provisions_Cost_USD INT,
    Guest_Rating_Stars INT -- 1 to 5
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Yachts
INSERT INTO Yacht_Life.Sources.Yacht_Registry VALUES
('Y-001', 'Sea Goddess', 60, 300000),
('Y-002', 'Ocean Pearl', 45, 150000),
('Y-003', 'Blue Horizon', 80, 500000);

-- Seed Charters
INSERT INTO Yacht_Life.Sources.Charter_Log VALUES
('C-001', 'Y-001', '2023-06-01', 'Monaco', 10000, 20000, 5),
('C-002', 'Y-001', '2023-06-08', 'St. Tropez', 5000, 15000, 4),
('C-003', 'Y-001', '2023-06-15', 'Cannes', 8000, 18000, 5),
('C-004', 'Y-002', '2023-06-01', 'Ibiza', 4000, 5000, 1), -- Party damage?
('C-005', 'Y-002', '2023-06-08', 'Mallorca', 3000, 4000, 3),
('C-006', 'Y-003', '2023-06-01', 'Portofino', 20000, 50000, 5), -- Huge spend
-- Fill 50
('C-007', 'Y-001', '2023-06-22', 'Monaco', 9000, 19000, 5),
('C-008', 'Y-001', '2023-06-29', 'Antibes', 2000, 10000, 5), -- Anchor days
('C-009', 'Y-001', '2023-07-06', 'Nice', 5000, 15000, 4),
('C-010', 'Y-001', '2023-07-13', 'Monaco', 10000, 25000, 5),
('C-011', 'Y-001', '2023-07-20', 'St. Tropez', 6000, 16000, 5),
('C-012', 'Y-001', '2023-07-27', 'Cannes', 7000, 17000, 5),
('C-013', 'Y-001', '2023-08-03', 'Monaco', 11000, 21000, 5),
('C-014', 'Y-001', '2023-08-10', 'Corsica', 15000, 12000, 5), -- Long trip
('C-015', 'Y-001', '2023-08-17', 'Sardinia', 14000, 13000, 4),
('C-016', 'Y-002', '2023-06-15', 'Ibiza', 4500, 5500, 2),
('C-017', 'Y-002', '2023-06-22', 'Formentera', 1000, 3000, 5),
('C-018', 'Y-002', '2023-06-29', 'Ibiza', 5000, 6000, 4),
('C-019', 'Y-002', '2023-07-06', 'Mallorca', 3000, 4000, 5),
('C-020', 'Y-002', '2023-07-13', 'Menorca', 2000, 3000, 5),
('C-021', 'Y-002', '2023-07-20', 'Barcelona', 6000, 7000, 3),
('C-022', 'Y-002', '2023-07-27', 'Valencia', 5000, 5000, 4),
('C-023', 'Y-003', '2023-06-08', 'Capri', 18000, 45000, 5),
('C-024', 'Y-003', '2023-06-15', 'Amalfi', 5000, 40000, 5),
('C-025', 'Y-003', '2023-06-22', 'Positano', 2000, 35000, 5),
('C-026', 'Y-003', '2023-06-29', 'Sorrento', 1000, 30000, 5),
('C-027', 'Y-003', '2023-07-06', 'Sicily', 25000, 40000, 4), -- Rough seas
('C-028', 'Y-003', '2023-07-13', 'Aeolian', 5000, 20000, 5),
('C-029', 'Y-003', '2023-07-20', 'Malta', 10000, 15000, 5),
('C-030', 'Y-003', '2023-07-27', 'Dubrovnik', 30000, 40000, 5),
('C-031', 'Y-001', '2023-09-01', 'Monaco', 5000, 10000, 5),
('C-032', 'Y-001', '2023-09-08', 'Monaco', 5000, 10000, 5),
('C-033', 'Y-001', '2023-09-15', 'Monaco', 0, 5000, 5), -- Stationary
('C-034', 'Y-002', '2023-08-01', 'Ibiza', 4000, 4000, 4),
('C-035', 'Y-002', '2023-08-08', 'Ibiza', 4000, 4000, 4),
('C-036', 'Y-002', '2023-08-15', 'Ibiza', 4000, 4000, 4),
('C-037', 'Y-002', '2023-08-22', 'Ibiza', 4000, 4000, 4),
('C-038', 'Y-003', '2023-08-01', 'Venice', 10000, 20000, 5),
('C-039', 'Y-003', '2023-08-08', 'Split', 5000, 10000, 5),
('C-040', 'Y-003', '2023-08-15', 'Hvar', 2000, 10000, 5),
('C-041', 'Y-001', '2023-05-01', 'Genoa', 0, 0, 3), -- Maintenance
('C-042', 'Y-001', '2023-05-08', 'Genoa', 0, 0, 3),
('C-043', 'Y-001', '2023-05-15', 'Genoa', 1000, 1000, 3),
('C-044', 'Y-001', '2023-05-22', 'Monaco', 2000, 5000, 4),
('C-045', 'Y-002', '2023-05-01', 'Palma', 0, 0, 3),
('C-046', 'Y-002', '2023-05-08', 'Palma', 0, 0, 3),
('C-047', 'Y-002', '2023-05-15', 'Palma', 1000, 500, 3),
('C-048', 'Y-003', '2023-05-01', 'Naples', 0, 0, 3),
('C-049', 'Y-003', '2023-05-08', 'Naples', 0, 0, 3),
('C-050', 'Y-003', '2023-05-15', 'Naples', 5000, 2000, 3);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Yacht_Life.Bronze.Bronze_Yachts AS SELECT * FROM Yacht_Life.Sources.Yacht_Registry;
CREATE OR REPLACE VIEW Yacht_Life.Bronze.Bronze_Charters AS SELECT * FROM Yacht_Life.Sources.Charter_Log;

-- 4b. SILVER LAYER (Cost Analysis)
CREATE OR REPLACE VIEW Yacht_Life.Silver.Silver_Trip_Economics AS
SELECT
    c.CharterID,
    y.Yacht_Name,
    c.Start_Date,
    c.Port_Location,
    c.Fuel_Added_Liters,
    -- Approx fuel cost $2/liter
    (c.Fuel_Added_Liters * 2.0) as Fuel_Cost_Est_USD,
    c.Provisions_Cost_USD,
    y.Weekly_Rate_USD as Charter_Fee_USD,
    -- Total Spend
    (y.Weekly_Rate_USD + (c.Fuel_Added_Liters * 2.0) + c.Provisions_Cost_USD) as Total_Trip_Cost_USD,
    c.Guest_Rating_Stars
FROM Yacht_Life.Bronze.Bronze_Charters c
JOIN Yacht_Life.Bronze.Bronze_Yachts y ON c.YachtID = y.YachtID;

-- 4c. GOLD LAYER (Yacht Performance)
CREATE OR REPLACE VIEW Yacht_Life.Gold.Gold_Top_Rated_Yachts AS
SELECT
    Yacht_Name,
    COUNT(*) as Charters_Completed,
    AVG(Guest_Rating_Stars) as Avg_Star_Rating,
    SUM(Total_Trip_Cost_USD) as Total_Revenue_Generated,
    -- Profitability Efficiency (Revenue per liter fuel)
    ROUND(SUM(Total_Trip_Cost_USD) / NULLIF(SUM(Fuel_Added_Liters), 0), 2) as Revenue_Per_Liter_Fuel
FROM Yacht_Life.Silver.Silver_Trip_Economics
GROUP BY Yacht_Name
ORDER BY Avg_Star_Rating DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "From 'Gold_Top_Rated_Yachts', identify which yacht generates the lowest 'Revenue_Per_Liter_Fuel'. 
 * Suggest if this is due to high fuel consumption or low charter rates by checking 'Silver_Trip_Economics'."
 */
