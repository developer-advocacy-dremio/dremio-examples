-------------------------------------------------------------------------------
-- Dremio SQL Function Examples: Date & Time Functions
-- 
-- This script demonstrates the usage of date and time manipulation functions.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

CREATE TABLE IF NOT EXISTS FunctionExamplesDB.Events (
    EventID INT,
    EventName VARCHAR,
    EventTimestamp TIMESTAMP,
    EventDate DATE
);

INSERT INTO FunctionExamplesDB.Events (EventID, EventName, EventTimestamp, EventDate) VALUES
(1, 'Server Start', TIMESTAMP '2025-01-01 08:30:00', DATE '2025-01-01'),
(2, 'User Login', TIMESTAMP '2025-01-01 09:15:20', DATE '2025-01-01'),
(3, 'Data Sync', TIMESTAMP '2025-01-02 12:00:00', DATE '2025-01-02'),
(4, 'System Maintenance', TIMESTAMP '2025-02-15 23:45:00', DATE '2025-02-15');

-------------------------------------------------------------------------------
-- 2. EXAMPLES: Date & Time Functions
-------------------------------------------------------------------------------

-- 2.1 Current Date and Time (CURRENT_DATE, CURRENT_TIMESTAMP, NOW)
SELECT 
    CURRENT_DATE AS Today,
    CURRENT_TIMESTAMP AS Now_Timestamp,
    NOW() AS Now_Alternative
FROM (VALUES(1));

-- 2.2 Extraction (EXTRACT, DATE_PART)
-- EXTRACT(part FROM source)
-- YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
SELECT 
    EventName,
    EventTimestamp,
    EXTRACT(YEAR FROM EventTimestamp) AS Event_Year,
    EXTRACT(MONTH FROM EventTimestamp) AS Event_Month,
    EXTRACT(DAY FROM EventTimestamp) AS Event_Day,
    EXTRACT(HOUR FROM EventTimestamp) AS Event_Hour,
    DATE_PART('minute', EventTimestamp) AS Event_Minute -- DATE_PART is often synonymous
FROM FunctionExamplesDB.Events;

-- 2.3 Date Arithmetic (DATE_ADD, DATE_SUB, TIMESTAMP_ADD)
-- Adding/Subtracting intervals.
SELECT 
    EventDate,
    DATE_ADD(EventDate, 7) AS Next_Week,
    DATE_SUB(EventDate, 1) AS Yesterday,
    TIMESTAMPADD(HOUR, 2, EventTimestamp) AS Plus_2_Hours
FROM FunctionExamplesDB.Events;

-- 2.4 Differences (TIMESTAMPDIFF)
-- TIMESTAMPDIFF(unit, start, end) returns the integer count of units.
SELECT 
    E1.EventName AS Start_Event,
    E2.EventName AS End_Event,
    E1.EventTimestamp AS Start_Time,
    E2.EventTimestamp AS End_Time,
    TIMESTAMPDIFF(MINUTE, E1.EventTimestamp, E2.EventTimestamp) AS Diff_Minutes,
    TIMESTAMPDIFF(HOUR, E1.EventTimestamp, E2.EventTimestamp) AS Diff_Hours,
    TIMESTAMPDIFF(DAY, E1.EventTimestamp, E2.EventTimestamp) AS Diff_Days
FROM FunctionExamplesDB.Events E1
JOIN FunctionExamplesDB.Events E2 ON E1.EventID = 1 AND E2.EventID = 3;

-- 2.5 Truncation (DATE_TRUNC)
-- Rounds down a timestamp to the specified precision (YEAR, MONTH, DAY, HOUR).
SELECT 
    EventTimestamp,
    DATE_TRUNC('MONTH', EventTimestamp) AS Start_Of_Month,
    DATE_TRUNC('DAY', EventTimestamp) AS Start_Of_Day,
    DATE_TRUNC('HOUR', EventTimestamp) AS Start_Of_Hour
FROM FunctionExamplesDB.Events;

-- 2.6 Formatting and Parsing (TO_CHAR, TO_DATE, TO_TIMESTAMP)
-- TO_CHAR converts date/time to string with format.
-- TO_DATE/TO_TIMESTAMP converts string to date/time.
SELECT 
    EventTimestamp,
    TO_CHAR(EventTimestamp, 'YYYY-MM-DD HH24:MI:SS') AS Formatted_String,
    TO_DATE('2023-12-25', 'YYYY-MM-DD') AS Parsed_Date,
    TO_TIMESTAMP('2023-12-25 10:30:00', 'YYYY-MM-DD HH24:MI:SS') AS Parsed_Timestamp
FROM FunctionExamplesDB.Events
LIMIT 1;

-- 2.7 Last and Next Day (LAST_DAY, NEXT_DAY)
SELECT 
    EventDate,
    LAST_DAY(EventDate) AS End_Of_Month,
    NEXT_DAY(EventDate, 'SUNDAY') AS Next_Sunday
FROM FunctionExamplesDB.Events;
