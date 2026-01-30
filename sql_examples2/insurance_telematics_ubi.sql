/*
 * InsureTech: Telematics Usage-Based Insurance
 * 
 * Scenario:
 * Calculating driver safety scores based on braking, acceleration, and cornering events to adjust insurance premiums.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS InsureTechDB;
CREATE FOLDER IF NOT EXISTS InsureTechDB.Telematics;
CREATE FOLDER IF NOT EXISTS InsureTechDB.Telematics.Bronze;
CREATE FOLDER IF NOT EXISTS InsureTechDB.Telematics.Silver;
CREATE FOLDER IF NOT EXISTS InsureTechDB.Telematics.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Sensor Data
-------------------------------------------------------------------------------

-- PolicyHolders Table
CREATE TABLE IF NOT EXISTS InsureTechDB.Telematics.Bronze.PolicyHolders (
    PolicyID VARCHAR,
    DriverName VARCHAR,
    VehicleModel VARCHAR,
    BasePremium DOUBLE
);

INSERT INTO InsureTechDB.Telematics.Bronze.PolicyHolders VALUES
('POL-001', 'Alice', 'Sedan', 1200.0),
('POL-002', 'Bob', 'SUV', 1400.0),
('POL-003', 'Charlie', 'Truck', 1600.0),
('POL-004', 'David', 'Sedan', 1200.0),
('POL-005', 'Eva', 'Coupe', 1500.0),
('POL-006', 'Frank', 'SUV', 1400.0),
('POL-007', 'Grace', 'Sedan', 1200.0),
('POL-008', 'Hank', 'Truck', 1600.0),
('POL-009', 'Ivy', 'Coupe', 1500.0),
('POL-010', 'Jack', 'Sedan', 1200.0),
('POL-011', 'Karen', 'SUV', 1400.0),
('POL-012', 'Leo', 'Truck', 1600.0),
('POL-013', 'Mia', 'Coupe', 1500.0),
('POL-014', 'Nick', 'Sedan', 1200.0),
('POL-015', 'Olivia', 'SUV', 1400.0),
('POL-016', 'Paul', 'Truck', 1600.0),
('POL-017', 'Quinn', 'Coupe', 1500.0),
('POL-018', 'Rose', 'Sedan', 1200.0),
('POL-019', 'Steve', 'SUV', 1400.0),
('POL-020', 'Tina', 'Truck', 1600.0),
('POL-021', 'Uma', 'Coupe', 1500.0),
('POL-022', 'Victor', 'Sedan', 1200.0),
('POL-023', 'Wendy', 'SUV', 1400.0),
('POL-024', 'Xavier', 'Truck', 1600.0),
('POL-025', 'Yara', 'Coupe', 1500.0),
('POL-026', 'Zack', 'Sedan', 1200.0),
('POL-027', 'Amy', 'SUV', 1400.0),
('POL-028', 'Ben', 'Truck', 1600.0),
('POL-029', 'Cathy', 'Coupe', 1500.0),
('POL-030', 'Dan', 'Sedan', 1200.0),
('POL-031', 'Erin', 'SUV', 1400.0),
('POL-032', 'Fred', 'Truck', 1600.0),
('POL-033', 'Gina', 'Coupe', 1500.0),
('POL-034', 'Harry', 'Sedan', 1200.0),
('POL-035', 'Iris', 'SUV', 1400.0),
('POL-036', 'John', 'Truck', 1600.0),
('POL-037', 'Kate', 'Coupe', 1500.0),
('POL-038', 'Liam', 'Sedan', 1200.0),
('POL-039', 'Mary', 'SUV', 1400.0),
('POL-040', 'Noah', 'Truck', 1600.0),
('POL-041', 'Olga', 'Coupe', 1500.0),
('POL-042', 'Pete', 'Sedan', 1200.0),
('POL-043', 'Queen', 'SUV', 1400.0),
('POL-044', 'Rob', 'Truck', 1600.0),
('POL-045', 'Sue', 'Coupe', 1500.0),
('POL-046', 'Tom', 'Sedan', 1200.0),
('POL-047', 'Ursula', 'SUV', 1400.0),
('POL-048', 'Vince', 'Truck', 1600.0),
('POL-049', 'Will', 'Coupe', 1500.0),
('POL-050', 'Xena', 'Sedan', 1200.0);

-- VehicleTelemetry Table
CREATE TABLE IF NOT EXISTS InsureTechDB.Telematics.Bronze.VehicleTelemetry (
    EventID INT,
    PolicyID VARCHAR,
    EventType VARCHAR, -- HardBrake, RapidAccel, Speeding, HardCorner
    Severity INT, -- 1-10
    EventTimestamp TIMESTAMP
);

INSERT INTO InsureTechDB.Telematics.Bronze.VehicleTelemetry VALUES
(1, 'POL-001', 'HardBrake', 8, '2025-06-01 08:30:00'),
(2, 'POL-001', 'Speeding', 5, '2025-06-01 08:35:00'),
(3, 'POL-002', 'RapidAccel', 6, '2025-06-01 09:00:00'),
(4, 'POL-003', 'HardCorner', 7, '2025-06-01 10:00:00'),
(5, 'POL-003', 'HardBrake', 9, '2025-06-01 10:05:00'), -- Aggressive
(6, 'POL-004', 'Speeding', 4, '2025-06-01 11:00:00'),
(7, 'POL-005', 'Speeding', 8, '2025-06-01 12:00:00'), -- High severity
(8, 'POL-005', 'RapidAccel', 8, '2025-06-01 12:05:00'),
(9, 'POL-005', 'HardCorner', 9, '2025-06-01 12:10:00'), -- Very aggressive
(10, 'POL-006', 'HardBrake', 5, '2025-06-01 13:00:00'),
(11, 'POL-007', 'Speeding', 3, '2025-06-01 14:00:00'),
(12, 'POL-008', 'HardCorner', 6, '2025-06-01 15:00:00'),
(13, 'POL-009', 'RapidAccel', 7, '2025-06-01 16:00:00'),
(14, 'POL-010', 'HardBrake', 4, '2025-06-01 17:00:00'),
(15, 'POL-011', 'Speeding', 5, '2025-06-01 18:00:00'),
(16, 'POL-012', 'Speeding', 2, '2025-06-01 19:00:00'),
(17, 'POL-013', 'HardCorner', 8, '2025-06-01 20:00:00'),
(18, 'POL-014', 'RapidAccel', 5, '2025-06-01 08:00:00'),
(19, 'POL-015', 'HardBrake', 6, '2025-06-01 09:00:00'),
(20, 'POL-016', 'Speeding', 4, '2025-06-01 10:00:00'),
(21, 'POL-017', 'HardCorner', 9, '2025-06-01 11:00:00'),
(22, 'POL-017', 'Speeding', 9, '2025-06-01 11:05:00'), -- Dangerous
(23, 'POL-018', 'RapidAccel', 3, '2025-06-01 12:00:00'),
(24, 'POL-019', 'HardBrake', 7, '2025-06-01 13:00:00'),
(25, 'POL-020', 'Speeding', 6, '2025-06-01 14:00:00'),
(26, 'POL-021', 'HardCorner', 5, '2025-06-01 15:00:00'),
(27, 'POL-022', 'RapidAccel', 4, '2025-06-01 16:00:00'),
(28, 'POL-023', 'HardBrake', 8, '2025-06-01 17:00:00'),
(29, 'POL-024', 'Speeding', 5, '2025-06-01 18:00:00'),
(30, 'POL-025', 'HardCorner', 7, '2025-06-01 19:00:00'),
(31, 'POL-026', 'RapidAccel', 2, '2025-06-01 20:00:00'),
(32, 'POL-027', 'HardBrake', 3, '2025-06-01 08:00:00'),
(33, 'POL-028', 'Speeding', 4, '2025-06-01 09:00:00'),
(34, 'POL-029', 'HardCorner', 5, '2025-06-01 10:00:00'),
(35, 'POL-030', 'RapidAccel', 6, '2025-06-01 11:00:00'),
(36, 'POL-031', 'HardBrake', 9, '2025-06-01 12:00:00'),
(37, 'POL-032', 'Speeding', 8, '2025-06-01 13:00:00'),
(38, 'POL-033', 'HardCorner', 7, '2025-06-01 14:00:00'),
(39, 'POL-034', 'RapidAccel', 5, '2025-06-01 15:00:00'),
(40, 'POL-035', 'HardBrake', 4, '2025-06-01 16:00:00'),
(41, 'POL-036', 'Speeding', 3, '2025-06-01 17:00:00'),
(42, 'POL-037', 'HardCorner', 2, '2025-06-01 18:00:00'),
(43, 'POL-038', 'RapidAccel', 8, '2025-06-01 19:00:00'),
(44, 'POL-039', 'HardBrake', 7, '2025-06-01 20:00:00'),
(45, 'POL-040', 'Speeding', 6, '2025-06-01 08:00:00'),
(46, 'POL-041', 'HardCorner', 5, '2025-06-01 09:00:00'),
(47, 'POL-042', 'RapidAccel', 4, '2025-06-01 10:00:00'),
(48, 'POL-043', 'HardBrake', 3, '2025-06-01 11:00:00'),
(49, 'POL-044', 'Speeding', 2, '2025-06-01 12:00:00'),
(50, 'POL-045', 'HardCorner', 9, '2025-06-01 13:00:00'), -- Dangerous
(51, 'POL-005', 'Speeding', 10, '2025-06-02 08:00:00'); -- Reckless

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Driving Behavior
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW InsureTechDB.Telematics.Silver.DriverRisk AS
SELECT 
    p.PolicyID,
    p.DriverName,
    p.BasePremium,
    COUNT(t.EventID) AS TotalEvents,
    AVG(t.Severity) AS AvgSeverity,
    SUM(CASE WHEN t.Severity > 7 THEN 1 ELSE 0 END) AS HighRiskEvents
FROM InsureTechDB.Telematics.Bronze.PolicyHolders p
LEFT JOIN InsureTechDB.Telematics.Bronze.VehicleTelemetry t ON p.PolicyID = t.PolicyID
GROUP BY p.PolicyID, p.DriverName, p.BasePremium;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Safety Scoring & Pricing
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW InsureTechDB.Telematics.Gold.PremiumAdjustments AS
SELECT 
    PolicyID,
    DriverName,
    BasePremium,
    TotalEvents,
    HighRiskEvents,
    CASE 
        WHEN TotalEvents = 0 THEN 'Safe Driver Discount'
        WHEN HighRiskEvents > 2 THEN 'High Risk Surcharge'
        WHEN HighRiskEvents > 0 THEN 'Moderate Risk'
        ELSE 'Standard Rate'
    END AS RatingCategory,
    CASE 
        WHEN TotalEvents = 0 THEN BasePremium * 0.90
        WHEN HighRiskEvents > 2 THEN BasePremium * 1.25
        WHEN HighRiskEvents > 0 THEN BasePremium * 1.10
        ELSE BasePremium
    END AS AdjustedPremium
FROM InsureTechDB.Telematics.Silver.DriverRisk;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Find all drivers who qualify for a 'Safe Driver Discount' in the InsureTechDB.Telematics.Gold.PremiumAdjustments view."

PROMPT 2:
"Calculate the average Severity of events for policies with 'Coupe' vehicle models using the Silver and Bronze layers."

PROMPT 3:
"List the top 3 riskiest drivers based on the HighRiskEvents count."
*/
