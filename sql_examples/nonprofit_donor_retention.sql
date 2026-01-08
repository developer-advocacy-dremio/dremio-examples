/*
 * Non-Profit Donor Retention Demo
 * 
 * Scenario:
 * A charity tracks donations to identify churned donors and campaign efficacy.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Increase donor lifetime value (LTV).
 * 
 * Note: Assumes a catalog named 'NonProfitDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS NonProfitDB;
CREATE FOLDER IF NOT EXISTS NonProfitDB.Bronze;
CREATE FOLDER IF NOT EXISTS NonProfitDB.Silver;
CREATE FOLDER IF NOT EXISTS NonProfitDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Donation Records
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS NonProfitDB.Bronze.Donors (
    DonorID INT,
    Name VARCHAR,
    Email VARCHAR,
    JoinDate DATE
);

CREATE TABLE IF NOT EXISTS NonProfitDB.Bronze.Campaigns (
    CampaignID INT,
    Title VARCHAR,
    TargetAmount DOUBLE
);

CREATE TABLE IF NOT EXISTS NonProfitDB.Bronze.Donations (
    DonationID INT,
    DonorID INT,
    CampaignID INT,
    DonationDate DATE,
    Amount DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO NonProfitDB.Bronze.Donors (DonorID, Name, Email, JoinDate) VALUES
(1, 'Alice Smith', 'alice@email.com', '2020-01-01'),
(2, 'Bob Jones', 'bob@email.com', '2023-05-15'),
(3, 'Charlie Day', 'charlie@email.com', '2019-11-20');

INSERT INTO NonProfitDB.Bronze.Campaigns (CampaignID, Title, TargetAmount) VALUES
(101, 'Clean Water Initiative', 50000.0),
(102, 'Education For All', 75000.0);

INSERT INTO NonProfitDB.Bronze.Donations (DonationID, DonorID, CampaignID, DonationDate, Amount) VALUES
(1, 1, 101, '2024-12-01', 100.0),
(2, 2, 101, '2024-12-05', 50.0),
(3, 1, 102, '2025-06-01', 200.0),
(4, 3, 101, '2020-02-01', 500.0); -- Inactive donor?

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Donor Activity
-------------------------------------------------------------------------------

-- 2.1 View: Donor_History
-- Calculates days since last donation.
CREATE OR REPLACE VIEW NonProfitDB.Silver.Donor_History AS
SELECT
    d.DonorID,
    d.Name,
    COUNT(do.DonationID) AS TotalDonations,
    SUM(do.Amount) AS LifetimeGiving,
    MAX(do.DonationDate) AS LastDonationDate,
    TIMESTAMPDIFF(DAY, MAX(do.DonationDate), CAST('2025-08-01' AS DATE)) AS DaysSinceLastGift
FROM NonProfitDB.Bronze.Donors d
LEFT JOIN NonProfitDB.Bronze.Donations do ON d.DonorID = do.DonorID
GROUP BY d.DonorID, d.Name;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Retention Segments
-------------------------------------------------------------------------------

-- 3.1 View: Donor_Segments
-- Segments donors by Recency.
CREATE OR REPLACE VIEW NonProfitDB.Gold.Donor_Segments AS
SELECT
    Name,
    LifetimeGiving,
    DaysSinceLastGift,
    CASE
        WHEN DaysSinceLastGift < 90 THEN 'Active'
        WHEN DaysSinceLastGift < 365 THEN 'Lapsing'
        ELSE 'Lapsed'
    END AS Status
FROM NonProfitDB.Silver.Donor_History;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Re-engagement):
"List all 'Lapsing' donors from NonProfitDB.Gold.Donor_Segments who have given > 100 dollars lifetime."

PROMPT 2 (Campaign Success):
"Which campaign has received the highest total donation amount based on NonProfitDB.Bronze.Donations?"
*/
