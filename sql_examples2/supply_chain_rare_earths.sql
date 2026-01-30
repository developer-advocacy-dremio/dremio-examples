/*
 * Supply Chain: Rare Earth Strategic Sourcing
 * 
 * Scenario:
 * Risk scoring suppliers of critical minerals (Neodymium, Lithium) based on geopolitical stability.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS StrategicMaterialsDB;
CREATE FOLDER IF NOT EXISTS StrategicMaterialsDB.Sourcing;
CREATE FOLDER IF NOT EXISTS StrategicMaterialsDB.Sourcing.Bronze;
CREATE FOLDER IF NOT EXISTS StrategicMaterialsDB.Sourcing.Silver;
CREATE FOLDER IF NOT EXISTS StrategicMaterialsDB.Sourcing.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Sourcing Data
-------------------------------------------------------------------------------

-- MineralSources Table
CREATE TABLE IF NOT EXISTS StrategicMaterialsDB.Sourcing.Bronze.MineralSources (
    ShipmentID VARCHAR,
    SupplierName VARCHAR,
    CountryOfOrigin VARCHAR,
    MineralType VARCHAR, -- Lithium, Cobalt, Neodymium
    WeightTons DOUBLE,
    ShipmentDate DATE
);

INSERT INTO StrategicMaterialsDB.Sourcing.Bronze.MineralSources VALUES
('SH-001', 'GlobalMining Corp', 'Australia', 'Lithium', 50.0, '2025-02-01'),
('SH-002', 'GlobalMining Corp', 'Australia', 'Lithium', 50.0, '2025-02-05'),
('SH-003', 'CongoCobalt Ltd', 'DRC', 'Cobalt', 20.0, '2025-02-02'),
('SH-004', 'CongoCobalt Ltd', 'DRC', 'Cobalt', 25.0, '2025-02-08'),
('SH-005', 'SinoRareEarths', 'China', 'Neodymium', 10.0, '2025-02-03'),
('SH-006', 'SinoRareEarths', 'China', 'Neodymium', 12.0, '2025-02-09'),
('SH-007', 'ChileLithium SA', 'Chile', 'Lithium', 40.0, '2025-02-04'),
('SH-008', 'ChileLithium SA', 'Chile', 'Lithium', 45.0, '2025-02-10'),
('SH-009', 'RusRareMetals', 'Russia', 'Palladium', 5.0, '2025-02-01'), -- High Risk
('SH-010', 'RusRareMetals', 'Russia', 'Palladium', 4.0, '2025-02-12'),
('SH-011', 'GlobalMining Corp', 'Australia', 'Lithium', 55.0, '2025-03-01'),
('SH-012', 'CongoCobalt Ltd', 'DRC', 'Cobalt', 22.0, '2025-03-02'),
('SH-013', 'SinoRareEarths', 'China', 'Neodymium', 15.0, '2025-03-03'),
('SH-014', 'ChileLithium SA', 'Chile', 'Lithium', 42.0, '2025-03-04'),
('SH-015', 'CanadaNickel', 'Canada', 'Nickel', 100.0, '2025-02-05'),
('SH-016', 'CanadaNickel', 'Canada', 'Nickel', 100.0, '2025-03-05'),
('SH-017', 'IndoNickel', 'Indonesia', 'Nickel', 80.0, '2025-02-06'),
('SH-018', 'IndoNickel', 'Indonesia', 'Nickel', 90.0, '2025-03-06'),
('SH-019', 'BrazilNiobium', 'Brazil', 'Niobium', 30.0, '2025-02-07'),
('SH-020', 'BrazilNiobium', 'Brazil', 'Niobium', 35.0, '2025-03-07'),
('SH-021', 'GlobalMining Corp', 'Australia', 'Lithium', 60.0, '2025-04-01'),
('SH-022', 'CongoCobalt Ltd', 'DRC', 'Cobalt', 20.0, '2025-04-02'),
('SH-023', 'SinoRareEarths', 'China', 'Neodymium', 10.0, '2025-04-03'),
('SH-024', 'ChileLithium SA', 'Chile', 'Lithium', 40.0, '2025-04-04'),
('SH-025', 'RusRareMetals', 'Russia', 'Palladium', 6.0, '2025-04-01'),
('SH-026', 'CanadaNickel', 'Canada', 'Nickel', 110.0, '2025-04-05'),
('SH-027', 'IndoNickel', 'Indonesia', 'Nickel', 85.0, '2025-04-06'),
('SH-028', 'BrazilNiobium', 'Brazil', 'Niobium', 32.0, '2025-04-07'),
('SH-029', 'USARareEarths', 'USA', 'Neodymium', 5.0, '2025-02-15'),
('SH-030', 'USARareEarths', 'USA', 'Neodymium', 6.0, '2025-03-15'),
('SH-031', 'GlobalMining Corp', 'Australia', 'Lithium', 52.0, '2025-05-01'),
('SH-032', 'CongoCobalt Ltd', 'DRC', 'Cobalt', 24.0, '2025-05-02'),
('SH-033', 'SinoRareEarths', 'China', 'Neodymium', 13.0, '2025-05-03'),
('SH-034', 'ChileLithium SA', 'Chile', 'Lithium', 44.0, '2025-05-04'),
('SH-035', 'RusRareMetals', 'Russia', 'Palladium', 3.0, '2025-05-01'),
('SH-036', 'CanadaNickel', 'Canada', 'Nickel', 105.0, '2025-05-05'),
('SH-037', 'IndoNickel', 'Indonesia', 'Nickel', 82.0, '2025-05-06'),
('SH-038', 'BrazilNiobium', 'Brazil', 'Niobium', 33.0, '2025-05-07'),
('SH-039', 'USARareEarths', 'USA', 'Neodymium', 4.0, '2025-04-15'),
('SH-040', 'VietRareEarths', 'Vietnam', 'Neodymium', 8.0, '2025-02-20'),
('SH-041', 'VietRareEarths', 'Vietnam', 'Neodymium', 9.0, '2025-03-20'),
('SH-042', 'GlobalMining Corp', 'Australia', 'Lithium', 58.0, '2025-06-01'),
('SH-043', 'CongoCobalt Ltd', 'DRC', 'Cobalt', 21.0, '2025-06-02'),
('SH-044', 'SinoRareEarths', 'China', 'Neodymium', 14.0, '2025-06-03'),
('SH-045', 'ChileLithium SA', 'Chile', 'Lithium', 46.0, '2025-06-04'),
('SH-046', 'RusRareMetals', 'Russia', 'Palladium', 7.0, '2025-06-01'),
('SH-047', 'CanadaNickel', 'Canada', 'Nickel', 115.0, '2025-06-05'),
('SH-048', 'IndoNickel', 'Indonesia', 'Nickel', 88.0, '2025-06-06'),
('SH-049', 'BrazilNiobium', 'Brazil', 'Niobium', 36.0, '2025-06-07'),
('SH-050', 'VietRareEarths', 'Vietnam', 'Neodymium', 10.0, '2025-04-20');

-- GeopoliticalRiskIndex Table
CREATE TABLE IF NOT EXISTS StrategicMaterialsDB.Sourcing.Bronze.GeopoliticalRiskIndex (
    Country VARCHAR,
    RiskScore INT, -- 1 (Safe) to 10 (High Risk)
    LastUpdated DATE
);

INSERT INTO StrategicMaterialsDB.Sourcing.Bronze.GeopoliticalRiskIndex VALUES
('Australia', 1, '2025-01-01'),
('Canada', 1, '2025-01-01'),
('USA', 1, '2025-01-01'),
('Chile', 3, '2025-01-01'),
('Brazil', 4, '2025-01-01'),
('Vietnam', 4, '2025-01-01'),
('Indonesia', 5, '2025-01-01'),
('China', 6, '2025-01-01'), -- Trade dispute risk
('DRC', 8, '2025-01-01'), -- Instability
('Russia', 10, '2025-01-01'); -- Sanctions

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Risk Assessment
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW StrategicMaterialsDB.Sourcing.Silver.ShipmentRisk AS
SELECT 
    s.ShipmentID,
    s.SupplierName,
    s.CountryOfOrigin,
    s.MineralType,
    s.WeightTons,
    r.RiskScore,
    (s.WeightTons * r.RiskScore) AS WeightedRiskExposure
FROM StrategicMaterialsDB.Sourcing.Bronze.MineralSources s
JOIN StrategicMaterialsDB.Sourcing.Bronze.GeopoliticalRiskIndex r ON s.CountryOfOrigin = r.Country;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Strategic Vulnerability
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW StrategicMaterialsDB.Sourcing.Gold.VulnerabilityDashboard AS
SELECT 
    MineralType,
    SUM(WeightTons) AS TotalVolume,
    AVG(RiskScore) AS AvgSupplyChainRisk,
    -- If > 50% of supply comes from Risk Score > 5, flag as Vulnerable
    SUM(CASE WHEN RiskScore > 5 THEN WeightTons ELSE 0 END) AS HighRiskVolume,
    (CAST(SUM(CASE WHEN RiskScore > 5 THEN WeightTons ELSE 0 END) AS DOUBLE) / SUM(WeightTons)) * 100 AS DependenceOnHighRiskPct
FROM StrategicMaterialsDB.Sourcing.Silver.ShipmentRisk
GROUP BY MineralType;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify which Mineral Type has the highest dependence on high-risk countries from the StrategicMaterialsDB.Sourcing.Gold.VulnerabilityDashboard view."

PROMPT 2:
"List all shipments originating from countries with a Risk Score greater than 7 in the Silver layer."

PROMPT 3:
"Show the total tonnage of 'Neodymium' sourced from 'China' vs 'USA'."
*/
