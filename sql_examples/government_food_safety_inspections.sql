/*
    Dremio High-Volume SQL Pattern: Government Food Safety Inspections
    
    Business Scenario:
    Health Departments inspect restaurants to ensure hygiene. "Letter Grades" (A, B, C) are
    calculated based on violation points. Tracking "Failed Inspections" prevents outbreaks.
    
    Data Story:
    We track Restaurant profiles and Inspection Logs.
    
    Medallion Architecture:
    - Bronze: Restaurants, Inspections.
      *Volume*: 50+ records.
    - Silver: InspectionScores (Summing violation points).
    - Gold: GradingResults (Mapping scores to A/B/C).
    
    Key Dremio Features:
    - Case Logic for Grading
    - Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentFoodDB;
CREATE FOLDER IF NOT EXISTS GovernmentFoodDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentFoodDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentFoodDB.Gold;
USE GovernmentFoodDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentFoodDB.Bronze.Restaurants (
    RestaurantID STRING,
    Name STRING,
    Cuisine STRING
);

INSERT INTO GovernmentFoodDB.Bronze.Restaurants VALUES
('R1', 'Joe Pizza', 'Pizza'),
('R2', 'Sushi World', 'Japanese'),
('R3', 'Burger King', 'Fast Food'),
('R4', 'Taco Stand', 'Mexican'),
('R5', 'Fine Dining Inc', 'French'),
-- Bulk Data (Expanded for Dremio Compatibility)
('R101', 'Generic Eatery 1', 'Diner'),
('R102', 'Generic Eatery 2', 'Bakery'),
('R103', 'Generic Eatery 3', 'Cafe'),
('R104', 'Generic Eatery 4', 'Diner'),
('R105', 'Generic Eatery 5', 'Bakery'),
('R106', 'Generic Eatery 6', 'Cafe'),
('R107', 'Generic Eatery 7', 'Diner'),
('R108', 'Generic Eatery 8', 'Bakery'),
('R109', 'Generic Eatery 9', 'Cafe'),
('R110', 'Generic Eatery 10', 'Diner'),
('R111', 'Generic Eatery 11', 'Bakery'),
('R112', 'Generic Eatery 12', 'Cafe'),
('R113', 'Generic Eatery 13', 'Diner'),
('R114', 'Generic Eatery 14', 'Bakery'),
('R115', 'Generic Eatery 15', 'Cafe'),
('R116', 'Generic Eatery 16', 'Diner'),
('R117', 'Generic Eatery 17', 'Bakery'),
('R118', 'Generic Eatery 18', 'Cafe'),
('R119', 'Generic Eatery 19', 'Diner'),
('R120', 'Generic Eatery 20', 'Bakery'),
('R121', 'Generic Eatery 21', 'Cafe'),
('R122', 'Generic Eatery 22', 'Diner'),
('R123', 'Generic Eatery 23', 'Bakery'),
('R124', 'Generic Eatery 24', 'Cafe'),
('R125', 'Generic Eatery 25', 'Diner'),
('R126', 'Generic Eatery 26', 'Bakery'),
('R127', 'Generic Eatery 27', 'Cafe'),
('R128', 'Generic Eatery 28', 'Diner'),
('R129', 'Generic Eatery 29', 'Bakery'),
('R130', 'Generic Eatery 30', 'Cafe'),
('R131', 'Generic Eatery 31', 'Diner'),
('R132', 'Generic Eatery 32', 'Bakery'),
('R133', 'Generic Eatery 33', 'Cafe'),
('R134', 'Generic Eatery 34', 'Diner'),
('R135', 'Generic Eatery 35', 'Bakery'),
('R136', 'Generic Eatery 36', 'Cafe'),
('R137', 'Generic Eatery 37', 'Diner'),
('R138', 'Generic Eatery 38', 'Bakery'),
('R139', 'Generic Eatery 39', 'Cafe'),
('R140', 'Generic Eatery 40', 'Diner'),
('R141', 'Generic Eatery 41', 'Bakery'),
('R142', 'Generic Eatery 42', 'Cafe'),
('R143', 'Generic Eatery 43', 'Diner'),
('R144', 'Generic Eatery 44', 'Bakery'),
('R145', 'Generic Eatery 45', 'Cafe'),
('R146', 'Generic Eatery 46', 'Diner'),
('R147', 'Generic Eatery 47', 'Bakery'),
('R148', 'Generic Eatery 48', 'Cafe'),
('R149', 'Generic Eatery 49', 'Diner'),
('R150', 'Generic Eatery 50', 'Bakery');

CREATE OR REPLACE TABLE GovernmentFoodDB.Bronze.Inspections (
    InspectionID STRING,
    RestaurantID STRING,
    Date DATE,
    ViolationCode STRING, -- V1 (Critical), V2 (Minor)
    Points INT
);

INSERT INTO GovernmentFoodDB.Bronze.Inspections VALUES
-- Linking to Bulk Restaurants (R101-R150)
('I501', 'R101', DATE '2025-01-15', 'V2_Minor', 2),
('I502', 'R102', DATE '2025-01-15', 'V2_Minor', 2),
('I503', 'R103', DATE '2025-01-15', 'V2_Minor', 2),
('I504', 'R104', DATE '2025-01-15', 'V2_Minor', 2),
('I505', 'R105', DATE '2025-01-15', 'V2_Minor', 2),
('I506', 'R106', DATE '2025-01-15', 'V2_Minor', 2),
('I507', 'R107', DATE '2025-01-15', 'V2_Minor', 2),
('I508', 'R108', DATE '2025-01-15', 'V2_Minor', 2),
('I509', 'R109', DATE '2025-01-15', 'V2_Minor', 2),
('I510', 'R110', DATE '2025-01-15', 'V1_Critical', 10), -- Critical every 10th
('I511', 'R111', DATE '2025-01-15', 'V2_Minor', 2),
('I512', 'R112', DATE '2025-01-15', 'V2_Minor', 2),
('I513', 'R113', DATE '2025-01-15', 'V2_Minor', 2),
('I514', 'R114', DATE '2025-01-15', 'V2_Minor', 2),
('I515', 'R115', DATE '2025-01-15', 'V2_Minor', 2),
('I516', 'R116', DATE '2025-01-15', 'V2_Minor', 2),
('I517', 'R117', DATE '2025-01-15', 'V2_Minor', 2),
('I518', 'R118', DATE '2025-01-15', 'V2_Minor', 2),
('I519', 'R119', DATE '2025-01-15', 'V2_Minor', 2),
('I520', 'R120', DATE '2025-01-15', 'V1_Critical', 10),
('I521', 'R121', DATE '2025-01-15', 'V2_Minor', 2),
('I522', 'R122', DATE '2025-01-15', 'V2_Minor', 2),
('I523', 'R123', DATE '2025-01-15', 'V2_Minor', 2),
('I524', 'R124', DATE '2025-01-15', 'V2_Minor', 2),
('I525', 'R125', DATE '2025-01-15', 'V2_Minor', 2),
('I526', 'R126', DATE '2025-01-15', 'V2_Minor', 2),
('I527', 'R127', DATE '2025-01-15', 'V2_Minor', 2),
('I528', 'R128', DATE '2025-01-15', 'V2_Minor', 2),
('I529', 'R129', DATE '2025-01-15', 'V2_Minor', 2),
('I530', 'R130', DATE '2025-01-15', 'V1_Critical', 10),
('I531', 'R131', DATE '2025-01-15', 'V2_Minor', 2),
('I532', 'R132', DATE '2025-01-15', 'V2_Minor', 2),
('I533', 'R133', DATE '2025-01-15', 'V2_Minor', 2),
('I534', 'R134', DATE '2025-01-15', 'V2_Minor', 2),
('I535', 'R135', DATE '2025-01-15', 'V2_Minor', 2),
('I536', 'R136', DATE '2025-01-15', 'V2_Minor', 2),
('I537', 'R137', DATE '2025-01-15', 'V2_Minor', 2),
('I538', 'R138', DATE '2025-01-15', 'V2_Minor', 2),
('I539', 'R139', DATE '2025-01-15', 'V2_Minor', 2),
('I540', 'R140', DATE '2025-01-15', 'V1_Critical', 10),
('I541', 'R141', DATE '2025-01-15', 'V2_Minor', 2),
('I542', 'R142', DATE '2025-01-15', 'V2_Minor', 2),
('I543', 'R143', DATE '2025-01-15', 'V2_Minor', 2),
('I544', 'R144', DATE '2025-01-15', 'V2_Minor', 2),
('I545', 'R145', DATE '2025-01-15', 'V2_Minor', 2),
('I546', 'R146', DATE '2025-01-15', 'V2_Minor', 2),
('I547', 'R147', DATE '2025-01-15', 'V2_Minor', 2),
('I548', 'R148', DATE '2025-01-15', 'V2_Minor', 2),
('I549', 'R149', DATE '2025-01-15', 'V2_Minor', 2),
('I550', 'R150', DATE '2025-01-15', 'V1_Critical', 10);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Scoring
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentFoodDB.Silver.DailyScores AS
SELECT
    RestaurantID,
    Date,
    SUM(Points) AS TotalViolationPoints
FROM GovernmentFoodDB.Bronze.Inspections
GROUP BY RestaurantID, Date;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Letter Grading
-------------------------------------------------------------------------------
-- NY Style: 0-13 = A, 14-27 = B, 28+ = C
CREATE OR REPLACE VIEW GovernmentFoodDB.Gold.RestaurantGrades AS
SELECT
    r.Name,
    r.Cuisine,
    s.Date,
    s.TotalViolationPoints,
    CASE 
        WHEN s.TotalViolationPoints <= 13 THEN 'A'
        WHEN s.TotalViolationPoints <= 27 THEN 'B'
        ELSE 'C'
    END AS Grade
FROM GovernmentFoodDB.Silver.DailyScores s
JOIN GovernmentFoodDB.Bronze.Restaurants r ON s.RestaurantID = r.RestaurantID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "How many restaurants received a 'C' grade?"
    2. "Show the average violation points by Cuisine."
    3. "List all Critical violations."
*/
