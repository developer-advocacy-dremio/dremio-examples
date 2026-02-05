/*
 * Dremio "Messy Data" Challenge: Waste Management Logistics
 * 
 * Scenario: 
 * Smart bin sensors.
 * 3 Tables: ZONES (Meta), BINS (Device), PICKUPS (Log).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: PICKUPS -> BINS -> ZONES.
 * 2. Optimize Route: Filter Bins with Fill_Level > 80.
 * 3. Detect Missed Pickups: Status='Missed'.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS City_Waste;
CREATE FOLDER IF NOT EXISTS City_Waste.Ops;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS City_Waste.Ops.ZONES (
    ZONE_ID VARCHAR,
    ZONE_NAME VARCHAR, -- 'North', 'South'
    TRUCK_ID VARCHAR
);

CREATE TABLE IF NOT EXISTS City_Waste.Ops.BINS (
    BIN_ID VARCHAR,
    ZONE_ID VARCHAR,
    CAPACITY_LITERS INT,
    TYPE VARCHAR -- 'Recycle', 'Trash'
);

CREATE TABLE IF NOT EXISTS City_Waste.Ops.PICKUPS (
    PICKUP_ID VARCHAR,
    BIN_ID VARCHAR,
    TS TIMESTAMP,
    FILL_LEVEL INT, -- 0-100
    STATUS VARCHAR -- 'Completed', 'Missed', 'Skipped'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- ZONES
INSERT INTO City_Waste.Ops.ZONES VALUES
('Z-1', 'Downtown', 'Trk-1'), ('Z-2', 'Suburbs', 'Trk-2'), ('Z-3', 'Industrial', 'Trk-3');

-- BINS
INSERT INTO City_Waste.Ops.BINS VALUES
('B-1', 'Z-1', 100, 'Trash'), ('B-2', 'Z-1', 100, 'Recycle'),
('B-3', 'Z-2', 200, 'Trash'), ('B-4', 'Z-2', 200, 'Recycle'),
('B-5', 'Z-3', 500, 'HazMat');

-- PICKUPS (50 Rows)
INSERT INTO City_Waste.Ops.PICKUPS VALUES
('P-1', 'B-1', '2023-01-01 08:00:00', 90, 'Completed'),
('P-2', 'B-2', '2023-01-01 08:05:00', 50, 'Completed'),
('P-3', 'B-1', '2023-01-02 08:00:00', 95, 'Completed'),
('P-4', 'B-1', '2023-01-03 08:00:00', 100, 'Completed'),
('P-5', 'B-1', '2023-01-04 08:00:00', 110, 'Completed'), -- Overflow
('P-6', 'B-1', '2023-01-05 08:00:00', 0, 'Skipped'), -- Empty
('P-7', 'B-3', '2023-01-01 09:00:00', 80, 'Missed'), -- Ops error
('P-8', 'B-3', '2023-01-02 09:00:00', 85, 'Missed'),
('P-9', 'B-3', '2023-01-03 09:00:00', 90, 'Completed'),
('P-10', 'B-4', '2023-01-01 09:10:00', 10, 'Skipped'),
('P-11', 'B-5', '2023-01-01 10:00:00', 50, 'Completed'),
('P-12', 'B-5', '2023-01-08 10:00:00', 60, 'Completed'),
('P-13', 'B-5', '2023-01-15 10:00:00', 70, 'Completed'),
('P-14', 'B-1', '2023-01-06 08:00:00', 40, 'Completed'),
('P-15', 'B-1', '2023-01-07 08:00:00', 45, 'Completed'),
('P-16', 'B-1', '2023-01-08 08:00:00', 50, 'Completed'),
('P-17', 'B-1', '2023-01-09 08:00:00', 55, 'Completed'),
('P-18', 'B-1', '2023-01-10 08:00:00', 60, 'Completed'),
('P-19', 'B-2', '2023-01-02 08:00:00', 55, 'Completed'),
('P-20', 'B-2', '2023-01-03 08:00:00', 60, 'Completed'),
('P-21', 'B-2', '2023-01-04 08:00:00', 65, 'Completed'),
('P-22', 'B-2', '2023-01-05 08:00:00', 70, 'Completed'),
('P-23', 'B-2', '2023-01-06 08:00:00', 75, 'Completed'),
('P-24', 'B-2', '2023-01-07 08:00:00', 80, 'Completed'),
('P-25', 'B-2', '2023-01-08 08:00:00', 85, 'Completed'),
('P-26', 'B-2', '2023-01-09 08:00:00', 90, 'Completed'),
('P-27', 'B-2', '2023-01-10 08:00:00', 95, 'Completed'),
('P-28', 'B-3', '2023-01-04 08:00:00', 50, 'Completed'),
('P-29', 'B-3', '2023-01-05 08:00:00', 50, 'Completed'),
('P-30', 'B-3', '2023-01-06 08:00:00', 50, 'Completed'),
('P-31', 'B-3', '2023-01-07 08:00:00', 50, 'Completed'),
('P-32', 'B-3', '2023-01-08 08:00:00', 50, 'Completed'),
('P-33', 'B-3', '2023-01-09 08:00:00', 50, 'Completed'),
('P-34', 'B-3', '2023-01-10 08:00:00', 50, 'Completed'),
('P-35', 'B-4', '2023-01-02 08:00:00', 0, 'Skipped'),
('P-36', 'B-4', '2023-01-03 08:00:00', 0, 'Skipped'),
('P-37', 'B-4', '2023-01-04 08:00:00', 0, 'Skipped'),
('P-38', 'B-4', '2023-01-05 08:00:00', 0, 'Skipped'),
('P-39', 'B-4', '2023-01-06 08:00:00', 0, 'Skipped'),
('P-40', 'B-4', '2023-01-07 08:00:00', 0, 'Skipped'),
('P-41', 'B-4', '2023-01-08 08:00:00', 0, 'Skipped'),
('P-42', 'B-4', '2023-01-09 08:00:00', 0, 'Skipped'),
('P-43', 'B-4', '2023-01-10 08:00:00', 0, 'Skipped'),
('P-44', 'B-5', '2023-01-02 08:00:00', 10, 'Completed'),
('P-45', 'B-5', '2023-01-03 08:00:00', 10, 'Completed'),
('P-46', 'B-5', '2023-01-04 08:00:00', 10, 'Completed'),
('P-47', 'B-5', '2023-01-05 08:00:00', 10, 'Completed'),
('P-48', 'B-5', '2023-01-06 08:00:00', 10, 'Completed'),
('P-49', 'B-5', '2023-01-07 08:00:00', 10, 'Completed'),
('P-50', 'B-5', '2023-01-08 08:00:00', 10, 'Completed');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Optimize trash collection in City_Waste.Ops.
 *  
 *  1. Bronze: Raw View of PICKUPS, BINS, ZONES.
 *  2. Silver: 
 *     - Join: PICKUPS -> BINS -> ZONES.
 *     - Efficiency: Flag Status='Skipped' OR Fill_Level < 20 as 'Inefficient'.
 *  3. Gold: 
 *     - Route Plan: Show specific Bins with Fill_Level > 80.
 *  
 *  Show the SQL."
 */
