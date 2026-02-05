/*
 * Dremio "Messy Data" Challenge: Voting Machine Logs
 * 
 * Scenario: 
 * Election logs from multiple Precincts.
 * 'Precinct_ID' formatting varies ('Precinct-1', '001', 'P1').
 * 'Timestamp' is unordered in the raw dump.
 * 'Vote_Type' includes 'Regular', 'Provisional', 'Adjudicated'.
 * 
 * Objective for AI Agent:
 * 1. Normalize Precinct ID (extract numeric).
 * 2. Filter out 'Adjudicated' votes (logic: these are resolved elsewhere).
 * 3. Count Votes per Candidate per Hour.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Election_HQ;
CREATE FOLDER IF NOT EXISTS Election_HQ.Ballot_Box;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Election_HQ.Ballot_Box.TALLY_LOG (
    VOTE_ID VARCHAR,
    PRECINCT_ID VARCHAR,
    CANDIDATE VARCHAR,
    VOTE_TYPE VARCHAR,
    TS TIMESTAMP
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (ID Variance, Timestamp Chaos)
-------------------------------------------------------------------------------

-- Precinct 1
INSERT INTO Election_HQ.Ballot_Box.TALLY_LOG VALUES
('V-001', 'Precinct-1', 'Candidate A', 'Regular', '2023-11-07 08:00:00'),
('V-002', 'Precinct-1', 'Candidate B', 'Regular', '2023-11-07 08:05:00');

-- Precinct 2 (Different ID format)
INSERT INTO Election_HQ.Ballot_Box.TALLY_LOG VALUES
('V-003', '002', 'Candidate A', 'Regular', '2023-11-07 08:10:00'),
('V-004', '002', 'Candidate B', 'Regular', '2023-11-07 08:15:00');

-- Precinct 3 (Third format)
INSERT INTO Election_HQ.Ballot_Box.TALLY_LOG VALUES
('V-005', 'P3', 'Candidate A', 'Provisional', '2023-11-07 08:20:00'),
('V-006', 'P3', 'Candidate B', 'Adjudicated', '2023-11-07 08:25:00'); -- Flagged

-- Unordered Timestamps
INSERT INTO Election_HQ.Ballot_Box.TALLY_LOG VALUES
('V-007', 'Precinct-1', 'Candidate A', 'Regular', '2023-11-07 07:55:00'), -- Before V-001
('V-008', '002', 'Candidate B', 'Regular', '2023-11-07 20:00:00'); -- Late night

-- Bulk Fill
INSERT INTO Election_HQ.Ballot_Box.TALLY_LOG VALUES
('V-100', 'Precinct-4', 'Candidate A', 'Regular', '2023-11-07 09:00:00'),
('V-101', 'Precinct-4', 'Candidate A', 'Regular', '2023-11-07 09:01:00'),
('V-102', 'Precinct-4', 'Candidate A', 'Regular', '2023-11-07 09:02:00'),
('V-103', 'Precinct-4', 'Candidate B', 'Regular', '2023-11-07 09:03:00'),
('V-104', 'Precinct-4', 'Candidate B', 'Regular', '2023-11-07 09:04:00'),
('V-105', '005', 'Candidate A', 'Regular', '2023-11-07 10:00:00'),
('V-106', '005', 'Candidate B', 'Regular', '2023-11-07 10:05:00'),
('V-107', '005', 'Candidate C', 'Write-In', '2023-11-07 10:10:00'),
('V-108', '005', 'Mickey Mouse', 'Write-In', '2023-11-07 10:15:00'), -- Invalid?
('V-109', 'P6', 'Candidate A', 'Regular', '2023-11-07 11:00:00'),
('V-110', 'P6', 'Candidate A', 'Regular', '2023-11-07 11:05:00'),
('V-111', 'P6', 'Candidate A', 'Regular', '2023-11-07 11:10:00'),
('V-112', 'P6', 'Candidate B', 'Adjudicated', '2023-11-07 11:15:00'),
('V-113', 'P6', 'Candidate B', 'Adjudicated', '2023-11-07 11:16:00'),
('V-114', 'Precinct-7', 'Candidate A', 'Regular', '2023-11-07 12:00:00'),
('V-115', 'Precinct-7', 'Candidate A', 'Regular', '2023-11-07 12:00:00'), -- Dupe V-ID?
('V-116', 'Precinct-7', 'Candidate B', 'Regular', '2023-11-07 12:05:00'),
('V-117', 'Precinct-8', 'Candidate A', 'Provisional', '2023-11-07 13:00:00'),
('V-118', 'Precinct-8', 'Candidate B', 'Provisional', '2023-11-07 13:05:00'),
('V-119', 'Precinct-9', 'Candidate A', 'Regular', '2023-11-07 14:00:00'),
('V-120', 'Precinct-9', 'Candidate B', 'Regular', '2023-11-07 14:00:00'),
('V-121', '010', 'Candidate A', 'Regular', '2023-11-07 15:00:00'),
('V-122', '010', 'Candidate A', 'Regular', '2023-11-07 15:01:00'),
('V-123', '010', 'Candidate A', 'Regular', '2023-11-07 15:02:00'),
('V-124', '010', 'Candidate A', 'Regular', '2023-11-07 15:03:00'),
('V-125', '010', 'Candidate A', 'Regular', '2023-11-07 15:04:00'),
('V-126', 'P11', 'Candidate B', 'Regular', '2023-11-07 16:00:00'),
('V-127', 'P11', 'Candidate B', 'Regular', '2023-11-07 16:01:00'),
('V-128', 'P11', 'Candidate B', 'Regular', '2023-11-07 16:02:00'),
('V-129', 'P11', 'Candidate B', 'Regular', '2023-11-07 16:03:00'),
('V-130', 'P11', 'Candidate A', 'Regular', '2023-11-07 16:04:00'),
('V-131', 'Precinct-12', 'Candidate A', 'Regular', '2023-11-07 17:00:00'),
('V-132', 'Precinct-12', 'Candidate B', 'Regular', '2023-11-07 17:05:00'),
('V-133', 'P13', 'NULL', 'Regular', '2023-11-07 18:00:00'), -- Null Candidate
('V-134', 'P13', '', 'Regular', '2023-11-07 18:05:00'), -- Empty Candidate
('V-135', 'P14', 'Candidate A', 'Spoiled', '2023-11-07 19:00:00'),
('V-136', 'P14', 'Candidate A', 'Spoiled', '2023-11-07 19:05:00'),
('V-137', '15', 'Candidate A', 'Regular', '2023-11-07 06:00:00'), -- Early vote?
('V-138', '15', 'Candidate B', 'Regular', '2023-11-07 22:00:00'), -- Late vote?
('V-139', '016', 'Candidate A', 'Regular', '2023-11-07 12:00:00'),
('V-140', '016', 'Candidate B', 'Regular', '2023-11-07 12:00:00');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Tally the votes in Election_HQ.Ballot_Box.TALLY_LOG.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean Precinct ID: Remove 'Precinct-', 'P', and leading zeros.
 *     - Filter 'Adjudicated' or 'Spoiled' ballots.
 *  3. Gold: 
 *     - Pivot Tally: Rows = Precinct_ID, Columns = Candidate_A_Count, Candidate_B_Count.
 *  
 *  Generate the SQL."
 */
