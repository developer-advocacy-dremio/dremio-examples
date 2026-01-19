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

INSERT INTO HealthcareCredsDB.Bronze.ProviderRoster VALUES
('PRV101', 'Dr. Smith', 'Cardiology'),
('PRV102', 'Dr. Jones', 'Ortho'),
('PRV103', 'Dr. Lee', 'PrimaryCare'),
('PRV104', 'Dr. Patel', 'Cardiology'),
('PRV105', 'Dr. Kim', 'Ortho'),
('PRV106', 'Dr. Garcia', 'PrimaryCare'),
('PRV107', 'Dr. Brown', 'Cardiology'),
('PRV108', 'Dr. White', 'Ortho'),
('PRV109', 'Dr. Green', 'PrimaryCare'),
('PRV110', 'Dr. Black', 'Cardiology'),
('PRV111', 'Dr. Grey', 'Ortho'),
('PRV112', 'Dr. Blue', 'PrimaryCare'),
('PRV113', 'Dr. Red', 'Cardiology'),
('PRV114', 'Dr. Orange', 'Ortho'),
('PRV115', 'Dr. Yellow', 'PrimaryCare'),
('PRV116', 'Dr. Purple', 'Cardiology'),
('PRV117', 'Dr. Pink', 'Ortho'),
('PRV118', 'Dr. Gold', 'PrimaryCare'),
('PRV119', 'Dr. Silver', 'Cardiology'),
('PRV120', 'Dr. Bronze', 'Ortho'),
('PRV121', 'Dr. Adams', 'PrimaryCare'),
('PRV122', 'Dr. Baker', 'Cardiology'),
('PRV123', 'Dr. Clark', 'Ortho'),
('PRV124', 'Dr. Davis', 'PrimaryCare'),
('PRV125', 'Dr. Evans', 'Cardiology'),
('PRV126', 'Dr. Ford', 'Ortho'),
('PRV127', 'Dr. Gomez', 'PrimaryCare'),
('PRV128', 'Dr. Hall', 'Cardiology'),
('PRV129', 'Dr. Ivy', 'Ortho'),
('PRV130', 'Dr. Jack', 'PrimaryCare'),
('PRV131', 'Dr. King', 'Cardiology'),
('PRV132', 'Dr. Lord', 'Ortho'),
('PRV133', 'Dr. Moon', 'PrimaryCare'),
('PRV134', 'Dr. Noon', 'Cardiology'),
('PRV135', 'Dr. Oates', 'Ortho'),
('PRV136', 'Dr. Park', 'PrimaryCare'),
('PRV137', 'Dr. Quinn', 'Cardiology'),
('PRV138', 'Dr. Rose', 'Ortho'),
('PRV139', 'Dr. Snow', 'PrimaryCare'),
('PRV140', 'Dr. Tate', 'Cardiology'),
('PRV141', 'Dr. Uzi', 'Ortho'),
('PRV142', 'Dr. Vann', 'PrimaryCare'),
('PRV143', 'Dr. West', 'Cardiology'),
('PRV144', 'Dr. Xray', 'Ortho'),
('PRV145', 'Dr. Yang', 'PrimaryCare'),
('PRV146', 'Dr. Zod', 'Cardiology'),
('PRV147', 'Dr. Alpha', 'Ortho'),
('PRV148', 'Dr. Beta', 'PrimaryCare'),
('PRV149', 'Dr. Gamma', 'Cardiology'),
('PRV150', 'Dr. Delta', 'Ortho');

CREATE OR REPLACE TABLE HealthcareCredsDB.Bronze.Credentials (
    CredID STRING,
    ProviderID STRING,
    Type STRING, -- STATE_LICENSE, DEA, BOARD_CERT
    ExpiryDate DATE
);

INSERT INTO HealthcareCredsDB.Bronze.Credentials VALUES
('C101', 'PRV101', 'STATE_LICENSE', DATE '2024-12-31'), -- Expired
('C102', 'PRV101', 'DEA', DATE '2025-06-01'),
('C103', 'PRV102', 'STATE_LICENSE', DATE '2025-02-01'), -- Expiring Soon
('C104', 'PRV102', 'DEA', DATE '2025-02-15'), -- Expiring Soon
('C105', 'PRV103', 'STATE_LICENSE', DATE '2025-12-31'),
('C106', 'PRV104', 'STATE_LICENSE', DATE '2025-03-01'), -- Valid
('C107', 'PRV105', 'STATE_LICENSE', DATE '2024-11-30'), -- Expired
('C108', 'PRV106', 'STATE_LICENSE', DATE '2026-01-01'),
('C109', 'PRV107', 'STATE_LICENSE', DATE '2025-01-20'), -- Valid (Just barely if today is Jan 1)
('C110', 'PRV108', 'STATE_LICENSE', DATE '2025-02-28'), -- Expiring Soon
('C111', 'PRV109', 'STATE_LICENSE', DATE '2025-12-31'),
('C112', 'PRV110', 'STATE_LICENSE', DATE '2025-04-01'),
('C113', 'PRV111', 'STATE_LICENSE', DATE '2024-10-15'), -- Expired
('C114', 'PRV112', 'STATE_LICENSE', DATE '2026-05-01'),
('C115', 'PRV113', 'STATE_LICENSE', DATE '2025-01-15'), -- Expiring Soon/Expired depending on exact date
('C116', 'PRV114', 'DEA', DATE '2025-03-15'),
('C117', 'PRV115', 'STATE_LICENSE', DATE '2025-12-01'),
('C118', 'PRV116', 'DEA', DATE '2024-12-01'), -- Expired
('C119', 'PRV117', 'STATE_LICENSE', DATE '2025-07-01'),
('C120', 'PRV118', 'STATE_LICENSE', DATE '2025-08-01'),
('C121', 'PRV119', 'STATE_LICENSE', DATE '2025-09-01'),
('C122', 'PRV120', 'DEA', DATE '2025-02-10'), -- Expiring Soon
('C123', 'PRV121', 'STATE_LICENSE', DATE '2025-10-01'),
('C124', 'PRV122', 'STATE_LICENSE', DATE '2025-11-01'),
('C125', 'PRV123', 'DEA', DATE '2024-09-01'), -- Expired
('C126', 'PRV124', 'STATE_LICENSE', DATE '2026-02-01'),
('C127', 'PRV125', 'STATE_LICENSE', DATE '2026-03-01'),
('C128', 'PRV126', 'DEA', DATE '2025-01-30'), -- Expiring Soon
('C129', 'PRV127', 'STATE_LICENSE', DATE '2026-04-01'),
('C130', 'PRV128', 'STATE_LICENSE', DATE '2026-05-01'),
('C131', 'PRV129', 'DEA', DATE '2025-05-01'),
('C132', 'PRV130', 'STATE_LICENSE', DATE '2026-06-01'),
('C133', 'PRV131', 'STATE_LICENSE', DATE '2024-12-25'), -- Expired
('C134', 'PRV132', 'DEA', DATE '2025-06-01'),
('C135', 'PRV133', 'STATE_LICENSE', DATE '2026-07-01'),
('C136', 'PRV134', 'STATE_LICENSE', DATE '2026-08-01'),
('C137', 'PRV135', 'DEA', DATE '2025-02-20'), -- Expiring Soon
('C138', 'PRV136', 'STATE_LICENSE', DATE '2026-09-01'),
('C139', 'PRV137', 'STATE_LICENSE', DATE '2026-10-01'),
('C140', 'PRV138', 'DEA', DATE '2025-07-01'),
('C141', 'PRV139', 'STATE_LICENSE', DATE '2024-08-01'), -- Expired
('C142', 'PRV140', 'STATE_LICENSE', DATE '2026-11-01'),
('C143', 'PRV141', 'DEA', DATE '2025-08-01'),
('C144', 'PRV142', 'STATE_LICENSE', DATE '2026-12-01'),
('C145', 'PRV143', 'STATE_LICENSE', DATE '2027-01-01'),
('C146', 'PRV144', 'DEA', DATE '2025-02-25'), -- Expiring Soon
('C147', 'PRV145', 'STATE_LICENSE', DATE '2027-02-01'),
('C148', 'PRV146', 'STATE_LICENSE', DATE '2027-03-01'),
('C149', 'PRV147', 'DEA', DATE '2024-12-15'), -- Expired
('C150', 'PRV148', 'STATE_LICENSE', DATE '2027-04-01'),
('C151', 'PRV149', 'STATE_LICENSE', DATE '2027-05-01'),
('C152', 'PRV150', 'DEA', DATE '2025-09-01');

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
