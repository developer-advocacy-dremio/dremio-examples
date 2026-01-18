/*
    Dremio High-Volume SQL Pattern: Government Voter Turnout
    
    Business Scenario:
    Elections Boards need to track voter registration vs actual turnout by precinct and 
    demographics to identify engagement gaps.
    
    Data Story:
    We track the Registered Voter rolls and Election Day check-ins.
    
    Medallion Architecture:
    - Bronze: VoterRolls, BallotCastHistory.
      *Volume*: 50+ records.
    - Silver: ParticipationStats (Matched turnout).
    - Gold: PrecinctAnalysis (Turnout % by Party/Age).
    
    Key Dremio Features:
    - Group By
    - Percentage Calc
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentVoterDB;
CREATE FOLDER IF NOT EXISTS GovernmentVoterDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentVoterDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentVoterDB.Gold;
USE GovernmentVoterDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentVoterDB.Bronze.VoterRolls (
    VoterID STRING,
    PrecinctID STRING,
    PartyAffiliation STRING, -- Dem, Rep, Ind
    AgeGroup STRING, -- 18-29, 30-45, 46-65, 65+
    RegistrationDate DATE
);

-- Bulk Voters (50 Records)
INSERT INTO GovernmentVoterDB.Bronze.VoterRolls VALUES
('V1001', 'Precinct_A', 'Dem', '18-29', DATE '2022-01-15'),
('V1002', 'Precinct_A', 'Rep', '30-45', DATE '2021-06-20'),
('V1003', 'Precinct_A', 'Ind', '46-65', DATE '2020-11-03'),
('V1004', 'Precinct_A', 'Dem', '65+', DATE '2019-05-12'),
('V1005', 'Precinct_A', 'Rep', '18-29', DATE '2023-02-10'),
('V1006', 'Precinct_B', 'Ind', '30-45', DATE '2021-08-30'),
('V1007', 'Precinct_B', 'Dem', '46-65', DATE '2020-03-25'),
('V1008', 'Precinct_B', 'Rep', '65+', DATE '2018-09-14'),
('V1009', 'Precinct_B', 'Dem', '18-29', DATE '2023-07-04'),
('V1010', 'Precinct_B', 'Rep', '30-45', DATE '2022-12-12'),
('V1011', 'Precinct_A', 'Ind', '46-65', DATE '2021-01-20'),
('V1012', 'Precinct_A', 'Dem', '65+', DATE '2019-04-18'),
('V1013', 'Precinct_A', 'Rep', '18-29', DATE '2023-01-01'),
('V1014', 'Precinct_A', 'Ind', '30-45', DATE '2020-07-04'),
('V1015', 'Precinct_A', 'Dem', '46-65', DATE '2021-09-09'),
('V1016', 'Precinct_B', 'Rep', '65+', DATE '2018-11-06'),
('V1017', 'Precinct_B', 'Ind', '18-29', DATE '2022-05-05'),
('V1018', 'Precinct_B', 'Dem', '30-45', DATE '2021-10-31'),
('V1019', 'Precinct_B', 'Rep', '46-65', DATE '2020-02-14'),
('V1020', 'Precinct_B', 'Dem', '65+', DATE '2019-08-01'),
('V1021', 'Precinct_A', 'Ind', '18-29', DATE '2023-03-15'),
('V1022', 'Precinct_A', 'Rep', '30-45', DATE '2022-04-01'),
('V1023', 'Precinct_A', 'Dem', '46-65', DATE '2021-06-15'),
('V1024', 'Precinct_A', 'Rep', '65+', DATE '2020-12-25'),
('V1025', 'Precinct_A', 'Ind', '18-29', DATE '2023-08-20'),
('V1026', 'Precinct_B', 'Dem', '30-45', DATE '2022-01-10'),
('V1027', 'Precinct_B', 'Rep', '46-65', DATE '2021-03-17'),
('V1028', 'Precinct_B', 'Ind', '65+', DATE '2019-11-11'),
('V1029', 'Precinct_B', 'Dem', '18-29', DATE '2023-09-05'),
('V1030', 'Precinct_B', 'Rep', '30-45', DATE '2022-02-28'),
('V1031', 'Precinct_A', 'Ind', '46-65', DATE '2021-05-05'),
('V1032', 'Precinct_A', 'Dem', '65+', DATE '2020-07-20'),
('V1033', 'Precinct_A', 'Rep', '18-29', DATE '2023-11-08'),
('V1034', 'Precinct_A', 'Ind', '30-45', DATE '2022-06-15'),
('V1035', 'Precinct_A', 'Dem', '46-65', DATE '2021-01-30'),
('V1036', 'Precinct_B', 'Rep', '65+', DATE '2018-12-01'),
('V1037', 'Precinct_B', 'Ind', '18-29', DATE '2023-04-20'),
('V1038', 'Precinct_B', 'Dem', '30-45', DATE '2022-09-10'),
('V1039', 'Precinct_B', 'Rep', '46-65', DATE '2021-12-31'),
('V1040', 'Precinct_B', 'Dem', '65+', DATE '2019-06-05'),
('V1041', 'Precinct_A', 'Ind', '18-29', DATE '2023-01-20'),
('V1042', 'Precinct_A', 'Rep', '30-45', DATE '2022-11-11'),
('V1043', 'Precinct_A', 'Dem', '46-65', DATE '2021-07-04'),
('V1044', 'Precinct_A', 'Rep', '65+', DATE '2020-03-01'),
('V1045', 'Precinct_A', 'Ind', '18-29', DATE '2023-10-10'),
('V1046', 'Precinct_B', 'Dem', '30-45', DATE '2022-05-30'),
('V1047', 'Precinct_B', 'Rep', '46-65', DATE '2021-08-15'),
('V1048', 'Precinct_B', 'Ind', '65+', DATE '2019-02-28'),
('V1049', 'Precinct_B', 'Dem', '18-29', DATE '2023-06-01'),
('V1050', 'Precinct_B', 'Rep', '30-45', DATE '2022-03-15');


CREATE OR REPLACE TABLE GovernmentVoterDB.Bronze.BallotCastHistory (
    BallotID STRING,
    VoterID STRING,
    ElectionDate DATE,
    Method STRING -- In-Person, Mail-In
);

-- Bulk Ballots (~35 Votes, approx 70% turnout)
INSERT INTO GovernmentVoterDB.Bronze.BallotCastHistory VALUES
('B5001', 'V1001', DATE '2024-11-05', 'In-Person'),
('B5002', 'V1002', DATE '2024-11-05', 'Mail-In'),
('B5003', 'V1003', DATE '2024-11-05', 'In-Person'),
('B5004', 'V1004', DATE '2024-11-05', 'Mail-In'),
('B5005', 'V1006', DATE '2024-11-05', 'In-Person'),
('B5006', 'V1007', DATE '2024-11-05', 'In-Person'),
('B5007', 'V1009', DATE '2024-11-05', 'Mail-In'),
('B5008', 'V1010', DATE '2024-11-05', 'In-Person'),
('B5009', 'V1012', DATE '2024-11-05', 'Mail-In'),
('B5010', 'V1013', DATE '2024-11-05', 'Mail-In'),
('B5011', 'V1015', DATE '2024-11-05', 'In-Person'),
('B5012', 'V1016', DATE '2024-11-05', 'In-Person'),
('B5013', 'V1017', DATE '2024-11-05', 'In-Person'),
('B5014', 'V1019', DATE '2024-11-05', 'Mail-In'),
('B5015', 'V1020', DATE '2024-11-05', 'Mail-In'),
('B5016', 'V1021', DATE '2024-11-05', 'In-Person'),
('B5017', 'V1022', DATE '2024-11-05', 'In-Person'),
('B5018', 'V1024', DATE '2024-11-05', 'Mail-In'),
('B5019', 'V1025', DATE '2024-11-05', 'In-Person'),
('B5020', 'V1028', DATE '2024-11-05', 'Mail-In'),
('B5021', 'V1029', DATE '2024-11-05', 'In-Person'),
('B5022', 'V1030', DATE '2024-11-05', 'Mail-In'),
('B5023', 'V1031', DATE '2024-11-05', 'In-Person'),
('B5024', 'V1033', DATE '2024-11-05', 'In-Person'),
('B5025', 'V1034', DATE '2024-11-05', 'Mail-In'),
('B5026', 'V1035', DATE '2024-11-05', 'In-Person'),
('B5027', 'V1038', DATE '2024-11-05', 'Mail-In'),
('B5028', 'V1039', DATE '2024-11-05', 'In-Person'),
('B5029', 'V1040', DATE '2024-11-05', 'Mail-In'),
('B5030', 'V1041', DATE '2024-11-05', 'In-Person'),
('B5031', 'V1044', DATE '2024-11-05', 'In-Person'),
('B5032', 'V1045', DATE '2024-11-05', 'Mail-In'),
('B5033', 'V1046', DATE '2024-11-05', 'In-Person'),
('B5034', 'V1048', DATE '2024-11-05', 'Mail-In'),
('B5035', 'V1049', DATE '2024-11-05', 'In-Person');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Participation Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentVoterDB.Silver.VoterParticipation AS
SELECT
    v.VoterID,
    v.PrecinctID,
    v.PartyAffiliation,
    v.AgeGroup,
    CASE WHEN b.BallotID IS NOT NULL THEN 1 ELSE 0 END AS VotedFlag,
    b.Method
FROM GovernmentVoterDB.Bronze.VoterRolls v
LEFT JOIN GovernmentVoterDB.Bronze.BallotCastHistory b 
    ON v.VoterID = b.VoterID AND b.ElectionDate = DATE '2024-11-05';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Turnout Dashboard
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentVoterDB.Gold.PrecinctTurnout AS
SELECT
    PrecinctID,
    PartyAffiliation,
    AgeGroup,
    COUNT(*) AS TotalRegistered,
    SUM(VotedFlag) AS TotalVotes,
    (CAST(SUM(VotedFlag) AS DOUBLE) / COUNT(*)) * 100 AS TurnoutPct
FROM GovernmentVoterDB.Silver.VoterParticipation
GROUP BY PrecinctID, PartyAffiliation, AgeGroup;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Party had the highest participation rate?"
    2. "Show voter turnout percentage for the 18-29 Age Group."
    3. "Compare In-Person vs Mail-In volume by Precinct."
*/
