/*
 * Agri-Finance: Parametric Weather Insurance
 * 
 * Scenario:
 * Automating insurance payouts for farmers based on regional rainfall index sensors (no claims processing needed).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AgriFinanceDB;
CREATE FOLDER IF NOT EXISTS AgriFinanceDB.Insurance;
CREATE FOLDER IF NOT EXISTS AgriFinanceDB.Insurance.Bronze;
CREATE FOLDER IF NOT EXISTS AgriFinanceDB.Insurance.Silver;
CREATE FOLDER IF NOT EXISTS AgriFinanceDB.Insurance.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Weather & Policy Data
-------------------------------------------------------------------------------

-- WeatherStations Table
CREATE TABLE IF NOT EXISTS AgriFinanceDB.Insurance.Bronze.WeatherStations (
    StationID VARCHAR,
    RegionID VARCHAR,
    DailyRainfallMM DOUBLE,
    WindSpeedKMH DOUBLE,
    ReadingDate DATE
);

INSERT INTO AgriFinanceDB.Insurance.Bronze.WeatherStations VALUES
('WS-001', 'REG-A', 0.0, 10.0, '2025-06-01'), -- Drought start?
('WS-001', 'REG-A', 0.0, 12.0, '2025-06-02'),
('WS-001', 'REG-A', 0.0, 15.0, '2025-06-03'),
('WS-001', 'REG-A', 2.0, 8.0, '2025-06-04'),
('WS-001', 'REG-A', 0.0, 10.0, '2025-06-05'),
('WS-002', 'REG-B', 15.0, 20.0, '2025-06-01'), -- Normal
('WS-002', 'REG-B', 12.0, 22.0, '2025-06-02'),
('WS-002', 'REG-B', 10.0, 18.0, '2025-06-03'),
('WS-002', 'REG-B', 5.0, 15.0, '2025-06-04'),
('WS-002', 'REG-B', 0.0, 10.0, '2025-06-05'),
('WS-001', 'REG-A', 0.0, 11.0, '2025-06-06'),
('WS-001', 'REG-A', 0.0, 13.0, '2025-06-07'),
('WS-001', 'REG-A', 0.0, 14.0, '2025-06-08'),
('WS-001', 'REG-A', 0.0, 12.0, '2025-06-09'),
('WS-001', 'REG-A', 0.0, 10.0, '2025-06-10'), -- 10 days mostly dry
('WS-003', 'REG-C', 50.0, 60.0, '2025-06-01'), -- Storm
('WS-003', 'REG-C', 45.0, 55.0, '2025-06-02'),
('WS-003', 'REG-C', 20.0, 40.0, '2025-06-03'),
('WS-003', 'REG-C', 10.0, 30.0, '2025-06-04'),
('WS-003', 'REG-C', 5.0, 20.0, '2025-06-05'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-01'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-02'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-03'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-04'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-05'),
('WS-005', 'REG-E', 3.0, 10.0, '2025-06-01'),
('WS-005', 'REG-E', 2.0, 10.0, '2025-06-02'),
('WS-005', 'REG-E', 4.0, 10.0, '2025-06-03'),
('WS-005', 'REG-E', 1.0, 10.0, '2025-06-04'),
('WS-005', 'REG-E', 2.0, 10.0, '2025-06-05'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-06'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-07'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-08'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-09'),
('WS-004', 'REG-D', 0.0, 5.0, '2025-06-10'), -- Severe Drought
('WS-002', 'REG-B', 2.0, 12.0, '2025-06-06'),
('WS-002', 'REG-B', 3.0, 14.0, '2025-06-07'),
('WS-002', 'REG-B', 1.0, 10.0, '2025-06-08'),
('WS-002', 'REG-B', 4.0, 15.0, '2025-06-09'),
('WS-002', 'REG-B', 2.0, 12.0, '2025-06-10'),
('WS-003', 'REG-C', 5.0, 15.0, '2025-06-06'),
('WS-003', 'REG-C', 5.0, 15.0, '2025-06-07'),
('WS-003', 'REG-C', 5.0, 15.0, '2025-06-08'),
('WS-003', 'REG-C', 5.0, 15.0, '2025-06-09'),
('WS-003', 'REG-C', 5.0, 15.0, '2025-06-10'),
('WS-005', 'REG-E', 2.0, 10.0, '2025-06-06'),
('WS-005', 'REG-E', 2.0, 10.0, '2025-06-07'),
('WS-005', 'REG-E', 2.0, 10.0, '2025-06-08'),
('WS-005', 'REG-E', 2.0, 10.0, '2025-06-09'),
('WS-005', 'REG-E', 2.0, 10.0, '2025-06-10');

-- Policies Table
CREATE TABLE IF NOT EXISTS AgriFinanceDB.Insurance.Bronze.Policies (
    PolicyID VARCHAR,
    FarmerID VARCHAR,
    RegionID VARCHAR,
    CoverageAmount DOUBLE,
    DroughtThresholdMM DOUBLE -- If rainfall < this for 10 days, pay
);

INSERT INTO AgriFinanceDB.Insurance.Bronze.Policies VALUES
('POL-100', 'F-001', 'REG-A', 50000.0, 5.0),
('POL-101', 'F-002', 'REG-A', 40000.0, 5.0),
('POL-102', 'F-003', 'REG-B', 60000.0, 5.0),
('POL-103', 'F-004', 'REG-C', 55000.0, 5.0),
('POL-104', 'F-005', 'REG-D', 30000.0, 2.0),
('POL-105', 'F-006', 'REG-D', 35000.0, 2.0),
('POL-106', 'F-007', 'REG-E', 45000.0, 5.0),
('POL-107', 'F-008', 'REG-A', 50000.0, 5.0),
('POL-108', 'F-009', 'REG-B', 60000.0, 5.0),
('POL-109', 'F-010', 'REG-C', 55000.0, 5.0),
('POL-110', 'F-011', 'REG-D', 30000.0, 2.0),
('POL-111', 'F-012', 'REG-E', 45000.0, 5.0),
('POL-112', 'F-013', 'REG-A', 50000.0, 5.0),
('POL-113', 'F-014', 'REG-B', 60000.0, 5.0),
('POL-114', 'F-015', 'REG-C', 55000.0, 5.0),
('POL-115', 'F-016', 'REG-D', 30000.0, 2.0),
('POL-116', 'F-017', 'REG-E', 45000.0, 5.0),
('POL-117', 'F-018', 'REG-A', 50000.0, 5.0),
('POL-118', 'F-019', 'REG-B', 60000.0, 5.0),
('POL-119', 'F-020', 'REG-C', 55000.0, 5.0),
('POL-120', 'F-021', 'REG-D', 30000.0, 2.0),
('POL-121', 'F-022', 'REG-E', 45000.0, 5.0),
('POL-122', 'F-023', 'REG-A', 50000.0, 5.0),
('POL-123', 'F-024', 'REG-B', 60000.0, 5.0),
('POL-124', 'F-025', 'REG-C', 55000.0, 5.0),
('POL-125', 'F-026', 'REG-D', 30000.0, 2.0),
('POL-126', 'F-027', 'REG-E', 45000.0, 5.0),
('POL-127', 'F-028', 'REG-A', 50000.0, 5.0),
('POL-128', 'F-029', 'REG-B', 60000.0, 5.0),
('POL-129', 'F-030', 'REG-C', 55000.0, 5.0),
('POL-130', 'F-031', 'REG-D', 30000.0, 2.0),
('POL-131', 'F-032', 'REG-E', 45000.0, 5.0),
('POL-132', 'F-033', 'REG-A', 50000.0, 5.0),
('POL-133', 'F-034', 'REG-B', 60000.0, 5.0),
('POL-134', 'F-035', 'REG-C', 55000.0, 5.0),
('POL-135', 'F-036', 'REG-D', 30000.0, 2.0),
('POL-136', 'F-037', 'REG-E', 45000.0, 5.0),
('POL-137', 'F-038', 'REG-A', 50000.0, 5.0),
('POL-138', 'F-039', 'REG-B', 60000.0, 5.0),
('POL-139', 'F-040', 'REG-C', 55000.0, 5.0),
('POL-140', 'F-041', 'REG-D', 30000.0, 2.0),
('POL-141', 'F-042', 'REG-E', 45000.0, 5.0),
('POL-142', 'F-043', 'REG-A', 50000.0, 5.0),
('POL-143', 'F-044', 'REG-B', 60000.0, 5.0),
('POL-144', 'F-045', 'REG-C', 55000.0, 5.0),
('POL-145', 'F-046', 'REG-D', 30000.0, 2.0),
('POL-146', 'F-047', 'REG-E', 45000.0, 5.0),
('POL-147', 'F-048', 'REG-A', 50000.0, 5.0),
('POL-148', 'F-049', 'REG-B', 60000.0, 5.0),
('POL-149', 'F-050', 'REG-C', 55000.0, 5.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Regional Aggregates
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AgriFinanceDB.Insurance.Silver.RegionalRainfall AS
SELECT 
    RegionID,
    SUM(DailyRainfallMM) AS TotalRainfall10Days,
    AVG(DailyRainfallMM) AS AvgRainfall,
    MAX(DailyRainfallMM) AS PeakRainfall
FROM AgriFinanceDB.Insurance.Bronze.WeatherStations
GROUP BY RegionID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Payout Trigger
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AgriFinanceDB.Insurance.Gold.PayoutTriggers AS
SELECT 
    p.PolicyID,
    p.FarmerID,
    r.RegionID,
    r.TotalRainfall10Days,
    p.DroughtThresholdMM,
    -- Payout Logic: Trigger if total 10-day rain < threshold * 10 (simplified)
    -- Actually, usually triggers if Cumulative Rain < X mm
    CASE 
        WHEN r.TotalRainfall10Days < (p.DroughtThresholdMM * 2) THEN 'TRIGGER PAYOUT' -- Very dry
        ELSE 'No Action'
    END AS PayoutStatus,
    CASE 
        WHEN r.TotalRainfall10Days < (p.DroughtThresholdMM * 2) THEN p.CoverageAmount
        ELSE 0.0
    END AS PayoutAmount
FROM AgriFinanceDB.Insurance.Bronze.Policies p
JOIN AgriFinanceDB.Insurance.Silver.RegionalRainfall r ON p.RegionID = r.RegionID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Calculate the total 'PayoutAmount' required for Region 'REG-D' in the Gold triggers view."

PROMPT 2:
"List all farmers eligible for a payout, including their rainfall totals."

PROMPT 3:
"Find the region with the lowest 10-day rainfall total from the Silver layer."
*/
