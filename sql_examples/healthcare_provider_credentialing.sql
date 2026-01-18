/*
    Dremio High-Volume SQL Pattern: Healthcare Provider Credentialing
    
    Business Scenario:
    Hospitals must ensure all providers (MDs, NPs) have active licenses and DEA registrations.
    Lapsed credentials leads to lost billing and legal risk.
    
    Data Story:
    We track a Provider roster and their License expirations.
    
    Medallion Architecture:
    - Bronze: ProviderRoster, Credentials.
      *Volume*: 50+ records.
    - Silver: ExpiryStatus (Days remaining).
    - Gold: ComplianceDashboard (Expiring < 60 days).
    
    Key Dremio Features:
    - TIMESTAMPDIFF
    - Status Banding
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareCredsDB;
CREATE FOLDER IF NOT EXISTS HealthcareCredsDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareCredsDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareCredsDB.Gold;
USE HealthcareCredsDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareCredsDB.Bronze.ProviderRoster (
    ProviderID STRING,
    Name STRING,
    Specialty STRING -- Cards, Ortho, Primary
);

-- Bulk Providers
INSERT INTO HealthcareCredsDB.Bronze.ProviderRoster
SELECT 
  'PRV' || CAST(rn + 100 AS STRING),
  'Dr. ' || CAST(rn + 100 AS STRING),
  CASE WHEN rn % 3 = 0 THEN 'Cardiology' WHEN rn % 3 = 1 THEN 'Ortho' ELSE 'PrimaryCare' END
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareCredsDB.Bronze.Credentials (
    CredID STRING,
    ProviderID STRING,
    Type STRING, -- STATE_LICENSE, DEA, BOARD_CERT
    ExpiryDate DATE
);

-- Bulk Credentials
INSERT INTO HealthcareCredsDB.Bronze.Credentials
SELECT 
  'C' || CAST(rn + 1000 AS STRING),
  'PRV' || CAST(rn + 100 AS STRING),
  CASE WHEN rn % 2 = 0 THEN 'STATE_LICENSE' ELSE 'DEA' END,
  DATE_ADD(DATE '2025-01-01', CAST((rn * 7) - 30 AS INT)) -- Spread expiries from -30 days to +300 days
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Expiry Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareCredsDB.Silver.CredentialStatus AS
SELECT
    p.Name,
    p.Specialty,
    c.Type,
    c.ExpiryDate,
    TIMESTAMPDIFF(DAY, DATE '2025-01-01', c.ExpiryDate) AS DaysRemaining,
    CASE 
        WHEN c.ExpiryDate < DATE '2025-01-01' THEN 'EXPIRED'
        WHEN TIMESTAMPDIFF(DAY, DATE '2025-01-01', c.ExpiryDate) <= 60 THEN 'EXPIRING_SOON'
        ELSE 'VALID'
    END AS Status
FROM HealthcareCredsDB.Bronze.Credentials c
JOIN HealthcareCredsDB.Bronze.ProviderRoster p ON c.ProviderID = p.ProviderID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Compliance Dashboard
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareCredsDB.Gold.ComplianceRiskReport AS
SELECT
    Status,
    COUNT(*) AS Count,
    ARRAY_AGG(Name) AS Providers
FROM HealthcareCredsDB.Silver.CredentialStatus
WHERE Status IN ('EXPIRED', 'EXPIRING_SOON')
GROUP BY Status;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all providers with Expired credentials."
    2. "Count expiring credentials by Specialty."
    3. "Show the total number of credentials expiring in the next 60 days."
*/
