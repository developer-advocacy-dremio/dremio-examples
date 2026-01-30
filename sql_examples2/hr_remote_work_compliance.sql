/*
 * HR: Remote Work & Tax Compliance
 * 
 * Scenario:
 * Tracking digital nomad employee locations via VPN/Login logs to ensure tax residency compliance (e.g., <183 days rule).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS WorkforceDB;
CREATE FOLDER IF NOT EXISTS WorkforceDB.Compliance;
CREATE FOLDER IF NOT EXISTS WorkforceDB.Compliance.Bronze;
CREATE FOLDER IF NOT EXISTS WorkforceDB.Compliance.Silver;
CREATE FOLDER IF NOT EXISTS WorkforceDB.Compliance.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Access Logs
-------------------------------------------------------------------------------

-- LoginEvents Table
CREATE TABLE IF NOT EXISTS WorkforceDB.Compliance.Bronze.LoginEvents (
    EventID VARCHAR,
    EmployeeID VARCHAR,
    LoginIP VARCHAR,
    GeoCountry VARCHAR, -- US, UK, ES, DE
    GeoCity VARCHAR,
    LoginTimestamp TIMESTAMP
);

INSERT INTO WorkforceDB.Compliance.Bronze.LoginEvents VALUES
('L-001', 'E-101', '192.168.1.1', 'US', 'New York', '2025-01-01 09:00:00'),
('L-002', 'E-101', '192.168.1.1', 'US', 'New York', '2025-01-02 09:00:00'),
('L-003', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-01-01 09:00:00'), -- nomad
('L-004', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-01-02 09:00:00'),
('L-005', 'E-103', '10.0.0.1', 'UK', 'London', '2025-01-01 09:00:00'),
('L-006', 'E-103', '10.0.0.1', 'UK', 'London', '2025-01-02 09:00:00'),
('L-007', 'E-102', '203.0.113.5', 'ES', 'Barcelona', '2025-02-01 09:00:00'), -- Still Spain
('L-008', 'E-102', '198.51.100.1', 'FR', 'Paris', '2025-06-01 09:00:00'), -- Moved
('L-009', 'E-104', '1.1.1.1', 'DE', 'Berlin', '2025-01-01 09:00:00'),
('L-010', 'E-104', '1.1.1.1', 'DE', 'Berlin', '2025-03-01 09:00:00'),
('L-011', 'E-105', '2.2.2.2', 'US', 'Austin', '2025-01-01 09:00:00'),
('L-012', 'E-105', '2.2.2.2', 'US', 'Austin', '2025-04-01 09:00:00'),
('L-013', 'E-101', '192.168.1.1', 'US', 'New York', '2025-06-01 09:00:00'),
('L-014', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-03-01 09:00:00'), -- Back in Spain?
('L-015', 'E-103', '10.0.0.1', 'UK', 'London', '2025-04-01 09:00:00'),
('L-016', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-04-01 09:00:00'),
('L-017', 'E-106', '3.3.3.3', 'IT', 'Rome', '2025-01-01 09:00:00'),
('L-018', 'E-106', '3.3.3.3', 'IT', 'Rome', '2025-02-01 09:00:00'),
('L-019', 'E-107', '4.4.4.4', 'JP', 'Tokyo', '2025-01-01 09:00:00'),
('L-020', 'E-107', '4.4.4.4', 'JP', 'Tokyo', '2025-02-01 09:00:00'),
('L-021', 'E-108', '5.5.5.5', 'US', 'Seattle', '2025-01-01 09:00:00'),
('L-022', 'E-108', '5.5.5.5', 'US', 'Seattle', '2025-05-01 09:00:00'),
('L-023', 'E-109', '6.6.6.6', 'BR', 'Rio', '2025-01-01 09:00:00'),
('L-024', 'E-109', '6.6.6.6', 'BR', 'Rio', '2025-06-01 09:00:00'),
('L-025', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-05-01 09:00:00'), -- Many days in Spain
('L-026', 'E-104', '1.1.1.1', 'DE', 'Berlin', '2025-05-01 09:00:00'),
('L-027', 'E-106', '3.3.3.3', 'IT', 'Rome', '2025-05-01 09:00:00'),
('L-028', 'E-110', '7.7.7.7', 'CA', 'Toronto', '2025-01-01 09:00:00'),
('L-029', 'E-110', '7.7.7.7', 'CA', 'Toronto', '2025-02-01 09:00:00'),
('L-030', 'E-111', '8.8.8.8', 'MX', 'Mexico City', '2025-01-01 09:00:00'),
('L-031', 'E-111', '8.8.8.8', 'MX', 'Mexico City', '2025-02-01 09:00:00'),
('L-032', 'E-112', '9.9.9.9', 'US', 'Miami', '2025-01-01 09:00:00'),
('L-033', 'E-112', '9.9.9.9', 'US', 'Miami', '2025-06-01 09:00:00'),
('L-034', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-07-01 09:00:00'),
('L-035', 'E-104', '1.1.1.1', 'DE', 'Berlin', '2025-07-01 09:00:00'),
('L-036', 'E-103', '10.0.0.1', 'UK', 'London', '2025-07-01 09:00:00'),
('L-037', 'E-113', '11.11.11.11', 'AU', 'Sydney', '2025-01-01 09:00:00'),
('L-038', 'E-113', '11.11.11.11', 'AU', 'Sydney', '2025-02-01 09:00:00'),
('L-039', 'E-114', '12.12.12.12', 'SG', 'Singapore', '2025-01-01 09:00:00'),
('L-040', 'E-114', '12.12.12.12', 'SG', 'Singapore', '2025-02-01 09:00:00'),
('L-041', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-08-01 09:00:00'),
('L-042', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-09-01 09:00:00'),
('L-043', 'E-104', '1.1.1.1', 'DE', 'Berlin', '2025-09-01 09:00:00'),
('L-044', 'E-103', '10.0.0.1', 'UK', 'London', '2025-09-01 09:00:00'),
('L-045', 'E-115', '13.13.13.13', 'ZA', 'Cape Town', '2025-01-01 09:00:00'),
('L-046', 'E-115', '13.13.13.13', 'ZA', 'Cape Town', '2025-02-01 09:00:00'),
('L-047', 'E-116', '14.14.14.14', 'IN', 'Bangalore', '2025-01-01 09:00:00'),
('L-048', 'E-116', '14.14.14.14', 'IN', 'Bangalore', '2025-02-01 09:00:00'),
('L-049', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-10-01 09:00:00'),
('L-050', 'E-102', '203.0.113.1', 'ES', 'Madrid', '2025-11-01 09:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Location Aggregation
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW WorkforceDB.Compliance.Silver.GeoResidency AS
SELECT 
    EmployeeID,
    GeoCountry,
    -- Extract Date to count unique active days
    CAST(LoginTimestamp AS DATE) AS LogDate
FROM WorkforceDB.Compliance.Bronze.LoginEvents
GROUP BY EmployeeID, GeoCountry, CAST(LoginTimestamp AS DATE);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Tax Risk Report
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW WorkforceDB.Compliance.Gold.TaxRiskFlags AS
SELECT 
    EmployeeID,
    GeoCountry,
    COUNT(DISTINCT LogDate) AS EstimatedActiveDays,
    CASE 
        WHEN COUNT(DISTINCT LogDate) > 183 THEN 'Tax Residency Triggered'
        WHEN COUNT(DISTINCT LogDate) > 150 THEN 'Warning: Approaching Threshold'
        ELSE 'Safe'
    END AS ComplianceStatus
FROM WorkforceDB.Compliance.Silver.GeoResidency
GROUP BY EmployeeID, GeoCountry;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all employees who have triggered a 'Tax Residency' compliance status in the Gold layer."

PROMPT 2:
"Count how many unique days 'E-102' was active in 'ES' (Spain)."

PROMPT 3:
"Show all login locations for EmployeeID 'E-104' from the Bronze logs."
*/
