/*
 * Dremio "Messy Data" Challenge: Messy Airline Logs
 * 
 * Scenario: 
 * Flight data aggregated from multiple regional systems.
 * Airport codes mix IATA (3-letter) and ICAO (4-letter).
 * Timestamps are in local time without offsets, leading to 'negative' flight durations.
 * 'Status' field is inconsistent ('Delayed', 'DLY', 'Cancelled', 'CX').
 * 
 * Objective for AI Agent:
 * 1. Normalize Airport Codes to IATA (substring 4-letter ICAO).
 * 2. Impute UTC offsets based on airport lookup (or assume UTC for simplification).
 * 3. Standardize Status codes.
 * 4. Calculate true Flight Duration.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Air_Traffic_Mess;
CREATE FOLDER IF NOT EXISTS Air_Traffic_Mess.Raw_Ingest;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Air_Traffic_Mess.Raw_Ingest.FLIGHT_LEGS (
    FL_ID VARCHAR,
    DEP_PORT VARCHAR, -- 'JFK', 'KJFK'
    ARR_PORT VARCHAR, -- 'LHR', 'EGLL'
    DEP_TS TIMESTAMP, -- Local Time
    ARR_TS TIMESTAMP, -- Local Time
    STATUS_RAW VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Code Mixing, Timezones)
-------------------------------------------------------------------------------

-- Normal IATA
INSERT INTO Air_Traffic_Mess.Raw_Ingest.FLIGHT_LEGS VALUES
('FL-101', 'JFK', 'LHR', '2023-01-01 18:00:00', '2023-01-02 06:00:00', 'On Time'),
('FL-102', 'LHR', 'CDG', '2023-01-01 10:00:00', '2023-01-01 12:00:00', 'Landed');

-- Mixed ICAO
INSERT INTO Air_Traffic_Mess.Raw_Ingest.FLIGHT_LEGS VALUES
('FL-103', 'KJFK', 'EGLL', '2023-01-01 19:00:00', '2023-01-02 07:00:00', 'Delayed'),
('FL-104', 'KLAX', 'RJTT', '2023-01-01 12:00:00', '2023-01-02 16:00:00', 'In Air');

-- Status Chaos
INSERT INTO Air_Traffic_Mess.Raw_Ingest.FLIGHT_LEGS VALUES
('FL-105', 'ORD', 'ATL', '2023-01-01 08:00:00', '2023-01-01 11:00:00', 'DLY'),
('FL-106', 'MIA', 'DFW', '2023-01-01 09:00:00', '2023-01-01 11:00:00', 'CX'), -- Cancelled
('FL-107', 'DEN', 'SFO', '2023-01-01 10:00:00', '2023-01-01 11:30:00', 'CANCELED'); -- Typo?

-- Timezone confusion (Arrival appears before departure if assumed UTC)
INSERT INTO Air_Traffic_Mess.Raw_Ingest.FLIGHT_LEGS VALUES
('FL-108', 'JFK', 'LAX', '2023-01-01 08:00:00', '2023-01-01 11:00:00', 'OK'); -- 3h difference nominal, but 6h flight

-- Bulk Fill
INSERT INTO Air_Traffic_Mess.Raw_Ingest.FLIGHT_LEGS VALUES
('FL-200', 'LHR', 'JFK', '2023-01-02 10:00:00', '2023-01-02 13:00:00', 'OK'),
('FL-201', 'EGLL', 'KJFK', '2023-01-02 11:00:00', '2023-01-02 14:00:00', 'OK'),
('FL-202', 'CDG', 'DXB', '2023-01-02 12:00:00', '2023-01-02 21:00:00', 'OK'),
('FL-203', 'LFPG', 'OMDB', '2023-01-02 12:00:00', '2023-01-02 21:00:00', 'OK'),
('FL-204', 'HND', 'SIN', '2023-01-02 10:00:00', '2023-01-02 16:00:00', 'OK'),
('FL-205', 'RJTT', 'WSSS', '2023-01-02 10:00:00', '2023-01-02 16:00:00', 'OK'),
('FL-206', 'SYD', 'LAX', '2023-01-02 10:00:00', '2023-01-02 06:00:00', 'OK'), -- Arrives 'before' dept
('FL-207', 'YSSY', 'KLAX', '2023-01-02 11:00:00', '2023-01-02 07:00:00', 'OK'),
('FL-208', 'BOM', 'DXB', '2023-01-02 10:00:00', '2023-01-02 12:00:00', 'OK'),
('FL-209', 'VABB', 'OMDB', '2023-01-02 10:00:00', '2023-01-02 12:00:00', 'OK'),
('FL-210', 'JFK', 'LHR', '2023-01-02 18:00', '2023-01-03 06:00', 'OT'), -- 'OT' status
('FL-211', 'JFK', 'LHR', '2023-01-02 19:00', '2023-01-03 07:00', 'DELAY'),
('FL-212', 'JFK', 'LHR', '2023-01-02 20:00', '2023-01-03 08:00', 'DLY'),
('FL-213', 'JFK', 'LHR', '2023-01-02 21:00', '2023-01-03 09:00', 'Unknown'),
('FL-214', 'JFK', 'LHR', '2023-01-02 22:00', '2023-01-03 10:00', NULL),
('FL-215', 'NRT', 'SFO', '2023-01-03 17:00:00', '2023-01-03 10:00:00', 'OK'), -- Time travel
('FL-216', 'RJAA', 'KSFO', '2023-01-03 18:00:00', '2023-01-03 11:00:00', 'OK'),
('FL-217', 'FRA', 'SIN', '2023-01-03 22:00:00', '2023-01-04 17:00:00', 'OK'),
('FL-218', 'EDDF', 'WSSS', '2023-01-03 22:00:00', '2023-01-04 17:00:00', 'OK'),
('FL-219', 'PEK', 'HKG', '2023-01-03 08:00:00', '2023-01-03 11:30:00', 'OK'),
('FL-220', 'ZBAA', 'VHHH', '2023-01-03 09:00:00', '2023-01-03 12:30:00', 'OK'),
('FL-221', 'AMS', 'JFK', '2023-01-03 10:00:00', '2023-01-03 12:00:00', 'cx'), -- lowercase
('FL-222', 'EHAM', 'KJFK', '2023-01-03 11:00:00', '2023-01-03 13:00:00', 'Cx'),
('FL-223', 'IST', 'LHR', '2023-01-03 10:00:00', '2023-01-03 12:00:00', 'Delayed'),
('FL-224', 'LTFM', 'EGLL', '2023-01-03 10:00:00', '2023-01-03 12:00:00', 'DELAYED'),
('FL-225', 'YYZ', 'YVR', '2023-01-03 08:00:00', '2023-01-03 10:00:00', 'OK'),
('FL-226', 'CYYZ', 'CYVR', '2023-01-03 09:00:00', '2023-01-03 11:00:00', 'OK'),
('FL-227', 'GRU', 'MIA', '2023-01-03 23:00:00', '2023-01-04 05:00:00', 'OK'),
('FL-228', 'SBGR', 'KMIA', '2023-01-03 23:00:00', '2023-01-04 05:00:00', 'OK'),
('FL-229', 'MEX', 'LAX', '2023-01-04 10:00:00', '2023-01-04 12:00:00', 'OK'),
('FL-230', 'MMMX', 'KLAX', '2023-01-04 11:00:00', '2023-01-04 13:00:00', 'OK'),
('FL-231', 'BKK', 'HND', '2023-01-04 23:00:00', '2023-01-05 07:00:00', 'OK'),
('FL-232', 'VTBS', 'RJTT', '2023-01-04 23:00:00', '2023-01-05 07:00:00', 'OK'),
('FL-233', 'DXB', 'JFK', '2023-01-04 02:00:00', '2023-01-04 08:00:00', 'OK'),
('FL-234', 'OMDB', 'KJFK', '2023-01-04 03:00:00', '2023-01-04 09:00:00', 'OK'),
('FL-235', 'MAD', 'EZE', '2023-01-04 23:55:00', '2023-01-05 08:00:00', 'OK'),
('FL-236', 'LEMD', 'SAEZ', '2023-01-04 23:55:00', '2023-01-05 08:00:00', 'OK'),
('FL-237', 'SVO', 'PVG', '2023-01-04 20:00:00', '2023-01-05 09:00:00', 'OK'),
('FL-238', 'UUEE', 'ZSPD', '2023-01-04 20:00:00', '2023-01-05 09:00:00', 'OK'),
('FL-239', 'ICN', 'LAX', '2023-01-05 15:00:00', '2023-01-05 09:00:00', 'OK'),
('FL-240', 'RKSI', 'KLAX', '2023-01-05 16:00:00', '2023-01-05 10:00:00', 'OK');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Process the flight logs in Air_Traffic_Mess.Raw_Ingest.FLIGHT_LEGS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Standardize Airport Codes: If 4 chars starting with 'K', 'E', 'R', etc., trim to last 3 (naive IATA conversion) or keep distinct.
 *     - Normalize Status: Map 'CX', 'CANCELED' -> 'Cancelled', 'DLY' -> 'Delayed'.
 *     - Detect 'Time Travel': Identify rows where Arrival < Departure (Timezone artifact).
 *  3. Gold: 
 *     - Count Flights by Route (Dep-Arr) and Status.
 *  
 *  Generate the SQL."
 */
