/*
 * Dremio Truffle Hunting Networks Example
 * 
 * Domain: Agriculture & Luxury Food
 * Scenario: 
 * Truffle hunters use Lagotto Romagnolo dogs to find white truffles.
 * They track "Soil pH", "Humidity", and "Dog Efficiency" (Finds per houur).
 * Auction prices fluctuate daily based on "Gram Weight" and "Aroma Class".
 * 
 * Complexity: Medium (Market volatility, biological efficacy)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Gourmet_Fungi;
CREATE FOLDER IF NOT EXISTS Gourmet_Fungi.Sources;
CREATE FOLDER IF NOT EXISTS Gourmet_Fungi.Bronze;
CREATE FOLDER IF NOT EXISTS Gourmet_Fungi.Silver;
CREATE FOLDER IF NOT EXISTS Gourmet_Fungi.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Gourmet_Fungi.Sources.Hunter_Profiles (
    HunterID VARCHAR, -- 'Mario', 'Luigi'
    Territory_Region VARCHAR, -- 'Piedmont', 'Tuscany'
    Dog_Name VARCHAR,
    Dog_Age INT
);

CREATE TABLE IF NOT EXISTS Gourmet_Fungi.Sources.Forage_Logs (
    LogID VARCHAR,
    HunterID VARCHAR,
    Forage_Date DATE,
    Soil_pH DOUBLE, -- 7.0 - 8.0 ideal
    Hours_Hunted DOUBLE,
    Truffles_Found_Count INT,
    Total_Weight_Grams DOUBLE,
    Est_Market_Value_EUR DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Hunters
INSERT INTO Gourmet_Fungi.Sources.Hunter_Profiles VALUES
('H-001', 'Piedmont', 'Bella', 4),
('H-002', 'Piedmont', 'Luna', 2), -- Puppy, less efficient
('H-003', 'Tuscany', 'Rocky', 6);

-- Seed Logs
INSERT INTO Gourmet_Fungi.Sources.Forage_Logs VALUES
('L-001', 'H-001', '2023-11-01', 7.5, 4.0, 3, 150.0, 450.0), -- Good day
('L-002', 'H-001', '2023-11-02', 7.4, 5.0, 4, 200.0, 600.0),
('L-003', 'H-002', '2023-11-01', 7.5, 4.0, 1, 30.0, 90.0), -- Puppy learning
('L-004', 'H-002', '2023-11-02', 7.6, 3.0, 0, 0.0, 0.0), -- Skunked
('L-005', 'H-003', '2023-11-01', 7.8, 6.0, 5, 250.0, 750.0),
-- Fill 50
('L-006', 'H-001', '2023-11-03', 7.5, 4.0, 2, 100.0, 300.0),
('L-007', 'H-001', '2023-11-04', 7.5, 4.0, 3, 150.0, 450.0),
('L-008', 'H-001', '2023-11-05', 7.5, 4.0, 1, 50.0, 150.0),
('L-009', 'H-001', '2023-11-06', 7.5, 4.0, 0, 0.0, 0.0),
('L-010', 'H-001', '2023-11-07', 7.5, 4.0, 4, 180.0, 540.0),
('L-011', 'H-002', '2023-11-03', 7.5, 4.0, 1, 40.0, 120.0),
('L-012', 'H-002', '2023-11-04', 7.5, 4.0, 1, 35.0, 105.0),
('L-013', 'H-002', '2023-11-05', 7.5, 4.0, 0, 0.0, 0.0),
('L-014', 'H-002', '2023-11-06', 7.5, 4.0, 2, 80.0, 240.0), -- Improving!
('L-015', 'H-002', '2023-11-07', 7.5, 4.0, 1, 40.0, 120.0),
('L-016', 'H-003', '2023-11-03', 7.8, 6.0, 4, 200.0, 600.0),
('L-017', 'H-003', '2023-11-04', 7.8, 6.0, 5, 220.0, 660.0),
('L-018', 'H-003', '2023-11-05', 7.8, 6.0, 2, 90.0, 270.0),
('L-019', 'H-003', '2023-11-06', 7.8, 6.0, 1, 40.0, 120.0),
('L-020', 'H-003', '2023-11-07', 7.8, 6.0, 0, 0.0, 0.0),
('L-021', 'H-001', '2023-11-08', 7.5, 4.0, 3, 150.0, 450.0),
('L-022', 'H-001', '2023-11-09', 7.5, 4.0, 2, 100.0, 300.0),
('L-023', 'H-001', '2023-11-10', 7.5, 4.0, 4, 200.0, 600.0),
('L-024', 'H-001', '2023-11-11', 7.5, 4.0, 1, 50.0, 150.0),
('L-025', 'H-001', '2023-11-12', 7.5, 4.0, 0, 0.0, 0.0),
('L-026', 'H-002', '2023-11-08', 7.5, 4.0, 2, 60.0, 180.0),
('L-027', 'H-002', '2023-11-09', 7.5, 4.0, 1, 30.0, 90.0),
('L-028', 'H-002', '2023-11-10', 7.5, 4.0, 0, 0.0, 0.0),
('L-029', 'H-002', '2023-11-11', 7.5, 4.0, 1, 40.0, 120.0),
('L-030', 'H-002', '2023-11-12', 7.5, 4.0, 2, 70.0, 210.0), -- Getting consistent
('L-031', 'H-003', '2023-11-08', 7.8, 6.0, 3, 150.0, 450.0),
('L-032', 'H-003', '2023-11-09', 7.8, 6.0, 4, 200.0, 600.0),
('L-033', 'H-003', '2023-11-10', 7.8, 6.0, 5, 250.0, 750.0),
('L-034', 'H-003', '2023-11-11', 7.8, 6.0, 2, 100.0, 300.0),
('L-035', 'H-003', '2023-11-12', 7.8, 6.0, 1, 50.0, 150.0),
('L-036', 'H-001', '2023-11-13', 7.5, 4.0, 3, 140.0, 420.0),
('L-037', 'H-001', '2023-11-14', 7.5, 4.0, 2, 90.0, 270.0),
('L-038', 'H-001', '2023-11-15', 7.5, 4.0, 4, 190.0, 570.0),
('L-039', 'H-001', '2023-11-16', 7.5, 4.0, 1, 40.0, 120.0),
('L-040', 'H-001', '2023-11-17', 7.5, 4.0, 0, 0.0, 0.0),
('L-041', 'H-002', '2023-11-13', 7.5, 4.0, 2, 75.0, 225.0),
('L-042', 'H-002', '2023-11-14', 7.5, 4.0, 1, 35.0, 105.0),
('L-043', 'H-002', '2023-11-15', 7.5, 4.0, 0, 0.0, 0.0),
('L-044', 'H-002', '2023-11-16', 7.5, 4.0, 1, 45.0, 135.0),
('L-045', 'H-002', '2023-11-17', 7.5, 4.0, 3, 100.0, 300.0), -- Breakout day!
('L-046', 'H-003', '2023-11-13', 7.8, 6.0, 3, 140.0, 420.0),
('L-047', 'H-003', '2023-11-14', 7.8, 6.0, 4, 190.0, 570.0),
('L-048', 'H-003', '2023-11-15', 7.8, 6.0, 5, 240.0, 720.0),
('L-049', 'H-003', '2023-11-16', 7.8, 6.0, 2, 95.0, 285.0),
('L-050', 'H-003', '2023-11-17', 7.8, 6.0, 1, 45.0, 135.0);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Gourmet_Fungi.Bronze.Bronze_Hunters AS SELECT * FROM Gourmet_Fungi.Sources.Hunter_Profiles;
CREATE OR REPLACE VIEW Gourmet_Fungi.Bronze.Bronze_Forage AS SELECT * FROM Gourmet_Fungi.Sources.Forage_Logs;

-- 4b. SILVER LAYER (Dog Efficiency)
CREATE OR REPLACE VIEW Gourmet_Fungi.Silver.Silver_Performance AS
SELECT
    f.Forage_Date,
    h.HunterID,
    h.Dog_Name,
    h.Dog_Age,
    f.Total_Weight_Grams,
    f.Hours_Hunted,
    -- Finds per Hour
    ROUND(f.Truffles_Found_Count / NULLIF(f.Hours_Hunted, 0), 2) as Finds_Per_Hour,
    -- Value per Hour
    ROUND(f.Est_Market_Value_EUR / NULLIF(f.Hours_Hunted, 0), 2) as Value_Created_Eur_Hr
FROM Gourmet_Fungi.Bronze.Bronze_Forage f
JOIN Gourmet_Fungi.Bronze.Bronze_Hunters h ON f.HunterID = h.HunterID;

-- 4c. GOLD LAYER (Leaderboard)
CREATE OR REPLACE VIEW Gourmet_Fungi.Gold.Gold_Top_Dogs AS
SELECT
    Dog_Name,
    Territory_Region,
    SUM(Total_Weight_Grams) as Seasonal_Yield_Grams,
    AVG(Finds_Per_Hour) as Avg_Efficiency,
    SUM(Est_Market_Value_EUR) as Total_Revenue
FROM Gourmet_Fungi.Silver.Silver_Performance
    JOIN Gourmet_Fungi.Bronze.Bronze_Hunters USING (HunterID)
GROUP BY Dog_Name, Territory_Region
ORDER BY Total_Revenue DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Compare 'Silver_Performance' for 'Bella' vs 'Luna'. 
 * Calculate the improvement curve for 'Luna' (the puppy) over the month of November based on 'Finds_Per_Hour'."
 */
