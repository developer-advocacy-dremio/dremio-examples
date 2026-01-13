/*
 * Museum Visitor Analytics Demo
 * 
 * Scenario:
 * Tracking ticket scans, exhibit dwell times (via WiFi/Bluetooth), and donor memberships.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize exhibit flow and increase membership conversions.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MuseumDB;
CREATE FOLDER IF NOT EXISTS MuseumDB.Bronze;
CREATE FOLDER IF NOT EXISTS MuseumDB.Silver;
CREATE FOLDER IF NOT EXISTS MuseumDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MuseumDB.Bronze.Tickets (
    TicketID VARCHAR,
    PurchaseDate DATE,
    VisitorType VARCHAR, -- 'Adult', 'Child', 'Senior', 'Member'
    EntryTime TIMESTAMP
);

CREATE TABLE IF NOT EXISTS MuseumDB.Bronze.ZoneTracking (
    TrackerID VARCHAR,
    ZoneName VARCHAR, -- 'DinosaurHall', 'ModernArt'
    TicketID VARCHAR,
    EnterTime TIMESTAMP,
    ExitTime TIMESTAMP
);

INSERT INTO MuseumDB.Bronze.Tickets VALUES
('T-101', '2025-07-01', 'Adult', '2025-07-01 10:00:00'),
('T-102', '2025-07-01', 'Member', '2025-07-01 10:15:00');

INSERT INTO MuseumDB.Bronze.ZoneTracking VALUES
('Z-01', 'DinosaurHall', 'T-101', '2025-07-01 10:10:00', '2025-07-01 10:55:00'),
('Z-02', 'ModernArt', 'T-102', '2025-07-01 10:20:00', '2025-07-01 10:50:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MuseumDB.Silver.ExhibitEngagement AS
SELECT 
    z.ZoneName,
    t.VisitorType,
    z.TicketID,
    TIMESTAMPDIFF(MINUTE, z.EnterTime, z.ExitTime) AS DwellTimeMinutes
FROM MuseumDB.Bronze.ZoneTracking z
JOIN MuseumDB.Bronze.Tickets t ON z.TicketID = t.TicketID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MuseumDB.Gold.ZonePopularity AS
SELECT 
    ZoneName,
    COUNT(DISTINCT TicketID) AS TotalVisitors,
    AVG(DwellTimeMinutes) AS AvgDwellTime
FROM MuseumDB.Silver.ExhibitEngagement
GROUP BY ZoneName;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which ZoneName has the highest AvgDwellTime in MuseumDB.Gold.ZonePopularity?"
*/
