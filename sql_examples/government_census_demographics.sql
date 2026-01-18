/*
    Dremio High-Volume SQL Pattern: Government Census Demographics
    
    Business Scenario:
    Planning Departments analyze "Population Shifts" to zone land and plan schools.
    Comparing Census Periods (e.g., 2010 vs 2020) reveals growth trends.
    
    Data Story:
    We track Census Tracts and Population Counts across two periods.
    
    Medallion Architecture:
    - Bronze: CensusTracts, PopulationData.
      *Volume*: 50+ records.
    - Silver: GrowthCalc (Delta between periods).
    - Gold: ShiftTrends (Fastest growing tracts).
    
    Key Dremio Features:
    - Self Join
    - Percentage Growth
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentCensusDB;
CREATE FOLDER IF NOT EXISTS GovernmentCensusDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentCensusDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentCensusDB.Gold;
USE GovernmentCensusDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentCensusDB.Bronze.CensusTracts (
    TractID STRING,
    DistrictName STRING,
    AreaSqMiles DOUBLE
);

INSERT INTO GovernmentCensusDB.Bronze.CensusTracts VALUES
('T101', 'North Dist', 5.5),
('T102', 'North Dist', 3.2),
('T103', 'North Dist', 4.1),
('T104', 'South Dist', 2.8),
('T105', 'South Dist', 6.0),
('T106', 'East Dist', 4.5),
('T107', 'East Dist', 5.1),
('T108', 'West Dist', 3.9),
('T109', 'West Dist', 4.2),
('T110', 'Central Dist', 1.5),
('T111', 'Central Dist', 1.8),
('T112', 'North Dist', 5.0),
('T113', 'South Dist', 6.2),
('T114', 'East Dist', 4.8),
('T115', 'West Dist', 4.0),
('T116', 'North Dist', 3.5),
('T117', 'South Dist', 2.9),
('T118', 'East Dist', 5.5),
('T119', 'West Dist', 4.1),
('T120', 'Central Dist', 2.0),
('T121', 'North Dist', 5.2),
('T122', 'South Dist', 6.5),
('T123', 'East Dist', 4.9),
('T124', 'West Dist', 3.8),
('T125', 'Central Dist', 1.9);

CREATE OR REPLACE TABLE GovernmentCensusDB.Bronze.PopulationData (
    RecordID STRING,
    TractID STRING,
    Year INT,
    Population INT
);

-- Bulk Pop Data (2010 and 2020 for 25 Tracts = 50 records)
INSERT INTO GovernmentCensusDB.Bronze.PopulationData VALUES
('REC1001', 'T101', 2010, 5000), ('REC1002', 'T101', 2020, 5500), -- 10% Growth
('REC1003', 'T102', 2010, 3000), ('REC1004', 'T102', 2020, 3100),
('REC1005', 'T103', 2010, 4000), ('REC1006', 'T103', 2020, 4500),
('REC1007', 'T104', 2010, 2500), ('REC1008', 'T104', 2020, 2400), -- Decline
('REC1009', 'T105', 2010, 6000), ('REC1010', 'T105', 2020, 6800),
('REC1011', 'T106', 2010, 4500), ('REC1012', 'T106', 2020, 4600),
('REC1013', 'T107', 2010, 5200), ('REC1014', 'T107', 2020, 5800),
('REC1015', 'T108', 2010, 3800), ('REC1016', 'T108', 2020, 4000),
('REC1017', 'T109', 2010, 4200), ('REC1018', 'T109', 2020, 4300),
('REC1019', 'T110', 2010, 1500), ('REC1020', 'T110', 2020, 2000), -- Big Gentrification
('REC1021', 'T111', 2010, 1800), ('REC1022', 'T111', 2020, 2200),
('REC1023', 'T112', 2010, 5100), ('REC1024', 'T112', 2020, 5200),
('REC1025', 'T113', 2010, 6200), ('REC1026', 'T113', 2020, 6300),
('REC1027', 'T114', 2010, 4800), ('REC1028', 'T114', 2020, 4850),
('REC1029', 'T115', 2010, 3900), ('REC1030', 'T115', 2020, 4100),
('REC1031', 'T116', 2010, 3500), ('REC1032', 'T116', 2020, 3600),
('REC1033', 'T117', 2010, 2900), ('REC1034', 'T117', 2020, 2800), -- Decline
('REC1035', 'T118', 2010, 5500), ('REC1036', 'T118', 2020, 6000),
('REC1037', 'T119', 2010, 4100), ('REC1038', 'T119', 2020, 4200),
('REC1039', 'T120', 2010, 2000), ('REC1040', 'T120', 2020, 2500),
('REC1041', 'T121', 2010, 5200), ('REC1042', 'T121', 2020, 5300),
('REC1043', 'T122', 2010, 6500), ('REC1044', 'T122', 2020, 6600),
('REC1045', 'T123', 2010, 4900), ('REC1046', 'T123', 2020, 5000),
('REC1047', 'T124', 2010, 3800), ('REC1048', 'T124', 2020, 3900),
('REC1049', 'T125', 2010, 1900), ('REC1050', 'T125', 2020, 2400);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Growth Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentCensusDB.Silver.TractGrowth AS
SELECT
    t.TractID,
    t.DistrictName,
    p1.Population AS Pop2010,
    p2.Population AS Pop2020,
    (p2.Population - p1.Population) AS NetChange,
    ((CAST(p2.Population AS DOUBLE) - p1.Population) / p1.Population) * 100 AS GrowthPct
FROM GovernmentCensusDB.Bronze.CensusTracts t
JOIN GovernmentCensusDB.Bronze.PopulationData p1 ON t.TractID = p1.TractID AND p1.Year = 2010
JOIN GovernmentCensusDB.Bronze.PopulationData p2 ON t.TractID = p2.TractID AND p2.Year = 2020;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: District Trends
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentCensusDB.Gold.DistrictDemographics AS
SELECT
    DistrictName,
    COUNT(*) AS TractCount,
    SUM(Pop2010) AS TotalPop2010,
    SUM(Pop2020) AS TotalPop2020,
    AVG(GrowthPct) AS AvgTractGrowth
FROM GovernmentCensusDB.Silver.TractGrowth
GROUP BY DistrictName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which District is growing faster?"
    2. "List tracts with > 10% growth."
    3. "Show total population in 2020 by District."
*/
