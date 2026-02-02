/*
 * Dremio Legal Patent Portfolio Mgmt Example
 * 
 * Domain: Legal Tech & Intellectual Property
 * Scenario: 
 * A top-tier law firm manages a massive portfolio of patents for various tech clients.
 * They need to track "Forward Citations" (who is citing our patents?) to value the portfolio 
 * for M&A activity. 
 * Additionally, they monitor "Expiration Cliffs" to advise clients on renewal vs. divestment strategies.
 * 
 * Complexity: Medium (Graph-like citation counting, date arithmetic)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data.
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Legal_IP;
CREATE FOLDER IF NOT EXISTS Legal_IP.Sources;
CREATE FOLDER IF NOT EXISTS Legal_IP.Bronze;
CREATE FOLDER IF NOT EXISTS Legal_IP.Silver;
CREATE FOLDER IF NOT EXISTS Legal_IP.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Legal_IP.Sources.Patent_Registry (
    PatentID VARCHAR,
    Title VARCHAR,
    Filing_Date DATE,
    Grant_Date DATE,
    Expiration_Date DATE,
    Status VARCHAR, -- 'Active', 'Expired', 'Pending'
    Owner_Entity VARCHAR
);

CREATE TABLE IF NOT EXISTS Legal_IP.Sources.Patent_Citations (
    CitationID VARCHAR,
    Citing_PatentID VARCHAR, -- The newer patent
    Cited_PatentID VARCHAR,  -- The older patent (our portfolio)
    Citation_Date DATE,
    Category VARCHAR -- 'Novelty', 'State of Art'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Registry
INSERT INTO Legal_IP.Sources.Patent_Registry VALUES
('US-9001', 'Autonomous Drone Navigation', '2010-01-01', '2012-06-01', '2030-01-01', 'Active', 'TechCorp'),
('US-9002', 'Solar Efficiency Coating',   '2010-02-01', '2012-07-01', '2030-02-01', 'Active', 'TechCorp'),
('US-9003', 'Blockchain Ledger Link',     '2015-01-01', '2017-06-01', '2035-01-01', 'Active', 'TechCorp'),
('US-9004', 'Wearable Health Sensor',     '2018-01-01', '2020-01-01', '2038-01-01', 'Active', 'MedTech'),
('US-9005', 'Quantum Error Correction',   '2019-01-01', '2021-01-01', '2039-01-01', 'Active', 'SciLab'),
('US-8001', 'Legacy Database Indexing',   '2002-01-01', '2004-01-01', '2022-01-01', 'Expired', 'OldSoft'),
('US-8002', 'CRT Monitor Refresh',        '2000-01-01', '2002-01-01', '2020-01-01', 'Expired', 'OldSoft'),
('US-9006', 'AI Neural Net Pruning',      '2021-01-01', '2022-06-01', '2041-01-01', 'Active', 'AIInc'),
('US-9007', 'Solid State Battery',        '2020-01-01', '2022-01-01', '2040-01-01', 'Active', 'AutoCo'),
('US-9008', 'Lidar Object Detection',     '2018-06-01', '2020-06-01', '2038-06-01', 'Active', 'AutoCo'),
('US-9009', 'VR Haptic Feedback',         '2019-06-01', '2021-06-01', '2039-06-01', 'Active', 'GameCo'),
('US-9010', '5G Antenna Array',           '2019-01-01', '2020-01-01', '2039-01-01', 'Active', 'TeleCom'),
('US-9011', 'Gene Editing CRISPR',        '2016-01-01', '2018-01-01', '2036-01-01', 'Active', 'BioGen'),
('US-9012', 'Smart Home Protocol',        '2017-01-01', '2019-01-01', '2037-01-01', 'Active', 'HomeInc'),
('US-9013', 'E-Ink Refresh Method',       '2015-06-01', '2017-06-01', '2035-06-01', 'Active', 'ReadCo'),
('US-9014', 'Foldable Screen Hinge',      '2019-01-01', '2020-06-01', '2039-01-01', 'Active', 'PhoneCo'),
('US-9015', 'Wireless Charging Pad',      '2014-01-01', '2016-01-01', '2034-01-01', 'Active', 'PhoneCo'),
('US-9016', 'Facial Recognition API',     '2017-06-01', '2019-06-01', '2037-06-01', 'Active', 'SecurCo'),
('US-9017', 'Drone Delivery Claw',        '2018-01-01', '2020-01-01', '2038-01-01', 'Active', 'LogistiCo'),
('US-9018', 'Vertical Farming Hydro',     '2020-01-01', '2021-06-01', '2040-01-01', 'Active', 'AgriTech'),
('US-9019', 'Desalination Membrane',      '2021-01-01', '2022-06-01', '2041-01-01', 'Active', 'WaterCo'),
('US-9020', 'Carbon Capture Matrix',      '2022-01-01', '2023-01-01', '2042-01-01', 'Active', 'GreenCo'),
('US-9021', 'Surgical Robot Arm',         '2018-01-01', '2019-01-01', '2038-01-01', 'Active', 'MedTech'),
('US-9022', 'Telemedicine Interface',     '2020-06-01', '2021-06-01', '2040-06-01', 'Active', 'MedTech'),
('US-9023', 'Smart Insoles',              '2019-01-01', '2020-01-01', '2039-01-01', 'Active', 'SportCo'),
('US-9024', 'Recyclable Plastic Polymer', '2021-06-01', '2023-01-01', '2041-06-01', 'Active', 'ChemCo'),
('US-9025', 'Audio Noise Cancellation',   '2016-01-01', '2018-01-01', '2036-01-01', 'Active', 'AudioInc'),
('US-9026', 'Video Compression Codec',    '2017-01-01', '2019-01-01', '2037-01-01', 'Active', 'StreamCo'),
('US-9027', 'Cloud Load Balancing',       '2015-01-01', '2017-01-01', '2035-01-01', 'Active', 'CloudInc'),
('US-9028', 'Cybersecurity Sandbox',      '2018-06-01', '2019-06-01', '2038-06-01', 'Active', 'SecurCo'),
('US-9029', 'Biometric Payment',          '2019-01-01', '2020-01-01', '2039-01-01', 'Active', 'FinTech'),
('US-9030', 'High Freq Trading Algo',     '2014-01-01', '2016-01-01', '2034-01-01', 'Active', 'FinTech'),
('US-9031', 'Robo Advisor Logic',         '2016-06-01', '2018-06-01', '2036-06-01', 'Active', 'FinTech'),
('US-9032', 'InsureTech Claims AI',       '2020-01-01', '2021-01-01', '2040-01-01', 'Active', 'InsureCo'),
('US-9033', 'Digital Twin Factory',       '2021-01-01', '2022-01-01', '2041-01-01', 'Active', 'InduCt'),
('US-9034', 'Smart Grid Metering',        '2017-01-01', '2019-01-01', '2037-01-01', 'Active', 'PowerCo'),
('US-9035', 'Wind Turbine Blade',         '2018-01-01', '2020-01-01', '2038-01-01', 'Active', 'PowerCo'),
('US-9036', 'Geothermal Drill Bit',       '2019-06-01', '2021-01-01', '2039-06-01', 'Active', 'PowerCo'),
('US-9037', 'Space Habitat Shield',       '2023-01-01', '2024-01-01', '2043-01-01', 'Active', 'SpaceXCo'),
('US-9038', 'Asteroid Mining Claw',       '2024-01-01', '2025-01-01', '2044-01-01', 'Pending', 'SpaceXCo'),
('US-9039', 'Fusion Reactor Liner',       '2022-06-01', '2024-01-01', '2042-06-01', 'Active', 'NukeCo'),
('US-9040', 'Algae Biofuel Vat',          '2020-01-01', '2021-06-01', '2040-01-01', 'Active', 'BioFuel'),
('US-9041', 'Lab Grown Meat Scaffold',    '2021-01-01', '2022-06-01', '2041-01-01', 'Active', 'FoodTech'),
('US-9042', 'Vertical Takeoff Jet',       '2019-01-01', '2021-01-01', '2039-01-01', 'Active', 'AeroCo'),
('US-9043', 'Hypersonic Scramjet',        '2020-06-01', '2022-01-01', '2040-06-01', 'Active', 'AeroCo'),
('US-9044', 'Underwater Drone Comm',      '2018-01-01', '2019-06-01', '2038-01-01', 'Active', 'MarineCo'),
('US-9045', 'Oil Spill Containment',      '2015-01-01', '2016-06-01', '2035-01-01', 'Active', 'MarineCo'),
('US-9046', 'Plastic Degrading Enzyme',   '2022-01-01', '2023-06-01', '2042-01-01', 'Active', 'BioClean'),
('US-9047', 'Wildfire Sensor Network',    '2021-06-01', '2022-06-01', '2041-06-01', 'Active', 'ForestCo'),
('US-9048', 'Earthquake Damper',          '2017-06-01', '2019-01-01', '2037-06-01', 'Active', 'BuildCo'),
('US-9049', 'Self Healing Concrete',      '2020-01-01', '2021-01-01', '2040-01-01', 'Active', 'BuildCo'),
('US-9050', 'Smart Window Tint',          '2019-01-01', '2020-06-01', '2039-01-01', 'Active', 'BuildCo');

-- Seed Citations (US-9001 drone patent heavily cited)
INSERT INTO Legal_IP.Sources.Patent_Citations VALUES
('CIT-01', 'US-9500', 'US-9001', '2012-12-01', 'Novelty'),
('CIT-02', 'US-9600', 'US-9001', '2013-01-01', 'Novelty'),
('CIT-03', 'US-9700', 'US-9001', '2013-06-01', 'State of Art'),
('CIT-04', 'US-9800', 'US-9001', '2014-01-01', 'Novelty'),
('CIT-05', 'US-9900', 'US-9001', '2015-01-01', 'State of Art'),
('CIT-06', 'US-9950', 'US-9003', '2018-01-01', 'Novelty'),
('CIT-07', 'US-9960', 'US-9003', '2018-02-01', 'Novelty'),
('CIT-08', 'US-9970', 'US-9004', '2021-01-01', 'State of Art'),
('CIT-09', 'US-9980', 'US-9006', '2023-01-01', 'Novelty'), -- AI citing AI
('CIT-10', 'US-9990', 'US-9006', '2023-06-01', 'Novelty'),
('CIT-11', 'US-9991', 'US-9001', '2016-01-01', 'Novelty'),
('CIT-12', 'US-9992', 'US-9001', '2016-06-01', 'Novelty'),
('CIT-13', 'US-9993', 'US-9001', '2017-01-01', 'Novelty'),
('CIT-14', 'US-9994', 'US-9002', '2013-01-01', 'State of Art'),
('CIT-15', 'US-9995', 'US-9002', '2014-01-01', 'Novelty'),
('CIT-16', 'US-9996', 'US-9005', '2022-01-01', 'State of Art'),
('CIT-17', 'US-9997', 'US-9005', '2022-06-01', 'Novelty'),
('CIT-18', 'US-9998', 'US-9007', '2023-01-01', 'Novelty'),
('CIT-19', 'US-9999', 'US-9008', '2021-01-01', 'State of Art'),
('CIT-20', 'US-10000', 'US-9009', '2022-01-01', 'Novelty'),
('CIT-21', 'US-10001', 'US-9009', '2023-01-01', 'Novelty'),
('CIT-22', 'US-10002', 'US-9010', '2021-06-01', 'State of Art'),
('CIT-23', 'US-10003', 'US-9011', '2019-01-01', 'Novelty'),
('CIT-24', 'US-10004', 'US-9012', '2020-01-01', 'Novelty'),
('CIT-25', 'US-10005', 'US-9001', '2018-01-01', 'Novelty'),
('CIT-26', 'US-10006', 'US-9013', '2018-01-01', 'State of Art'),
('CIT-27', 'US-10007', 'US-9014', '2021-01-01', 'Novelty'),
('CIT-28', 'US-10008', 'US-9015', '2017-01-01', 'State of Art'),
('CIT-29', 'US-10009', 'US-9016', '2020-01-01', 'Novelty'),
('CIT-30', 'US-10010', 'US-9017', '2021-01-01', 'Novelty'),
('CIT-31', 'US-10011', 'US-9018', '2022-01-01', 'State of Art'),
('CIT-32', 'US-10012', 'US-9019', '2023-01-01', 'Novelty'),
('CIT-33', 'US-10013', 'US-9020', '2023-06-01', 'Novelty'),
('CIT-34', 'US-10014', 'US-9021', '2020-01-01', 'State of Art'),
('CIT-35', 'US-10015', 'US-9022', '2022-01-01', 'Novelty'),
('CIT-36', 'US-10016', 'US-9023', '2021-01-01', 'Novelty'),
('CIT-37', 'US-10017', 'US-9024', '2023-06-01', 'State of Art'),
('CIT-38', 'US-10018', 'US-9025', '2019-01-01', 'Novelty'),
('CIT-39', 'US-10019', 'US-9026', '2020-01-01', 'Novelty'),
('CIT-40', 'US-10020', 'US-9027', '2018-01-01', 'State of Art'),
('CIT-41', 'US-10021', 'US-9028', '2020-01-01', 'Novelty'),
('CIT-42', 'US-10022', 'US-9029', '2021-01-01', 'Novelty'),
('CIT-43', 'US-10023', 'US-9030', '2017-01-01', 'State of Art'),
('CIT-44', 'US-10024', 'US-9031', '2019-01-01', 'Novelty'),
('CIT-45', 'US-10025', 'US-9032', '2022-01-01', 'Novelty'),
('CIT-46', 'US-10026', 'US-9033', '2023-01-01', 'State of Art'),
('CIT-47', 'US-10027', 'US-9001', '2019-01-01', 'Novelty'), -- Persistent interest in 9001
('CIT-48', 'US-10028', 'US-9001', '2020-01-01', 'Novelty'),
('CIT-49', 'US-10029', 'US-9035', '2021-01-01', 'Novelty'),
('CIT-50', 'US-10030', 'US-9036', '2022-06-01', 'State of Art');

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Legal_IP.Bronze.Bronze_Patents AS
SELECT
    PatentID,
    Title,
    Filing_Date,
    Grant_Date,
    Expiration_Date,
    Status,
    Owner_Entity
FROM Legal_IP.Sources.Patent_Registry;

CREATE OR REPLACE VIEW Legal_IP.Bronze.Bronze_Citations AS
SELECT
    CitationID,
    Citing_PatentID,
    Cited_PatentID,
    Citation_Date,
    Category
FROM Legal_IP.Sources.Patent_Citations;

-- 4b. SILVER LAYER (Forward Impact Count)
CREATE OR REPLACE VIEW Legal_IP.Silver.Silver_Patent_Impact AS
SELECT
    p.PatentID,
    p.Title,
    p.Status,
    p.Owner_Entity,
    p.Expiration_Date,
    DATEDIFF(p.Expiration_Date, CURRENT_DATE) as Days_Until_Expiry,
    COUNT(c.Citing_PatentID) as Forward_Citation_Count
FROM Legal_IP.Bronze.Bronze_Patents p
LEFT JOIN Legal_IP.Bronze.Bronze_Citations c ON p.PatentID = c.Cited_PatentID
GROUP BY p.PatentID, p.Title, p.Status, p.Owner_Entity, p.Expiration_Date;

-- 4c. GOLD LAYER (Portfolio Valuation)
CREATE OR REPLACE VIEW Legal_IP.Gold.Gold_Portfolio_Scorecard AS
SELECT
    Owner_Entity,
    COUNT(*) as Patent_Count,
    SUM(Forward_Citation_Count) as Total_Portfolio_Impact,
    AVG(Forward_Citation_Count) as Avg_Impact_Per_Patent,
    SUM(CASE WHEN Days_Until_Expiry BETWEEN 0 AND 365 THEN 1 ELSE 0 END) as Expirations_This_Year
FROM Legal_IP.Silver.Silver_Patent_Impact
WHERE Status = 'Active'
GROUP BY Owner_Entity
ORDER BY Total_Portfolio_Impact DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Examine 'Gold_Portfolio_Scorecard' for entities with high 'Expirations_This_Year'. 
 * Recommend renewal or divestment based on 'Avg_Impact_Per_Patent' falling below industry benchmarks (e.g., < 2.0)."
 */
