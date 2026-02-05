/*
 * Dremio "Messy Data" Challenge: Fragmented Logistics
 * 
 * Scenario: 
 * Tracking data is split between a "Legacy System" (Shipments A) and a "New System" (Shipments B).
 * Some shipments exist in both (Overlap).
 * Status codes are different ('In-Transit' vs 'IT', 'Delivered' vs 'DEL').
 * 
 * Objective for AI Agent:
 * 1. Union the two tables.
 * 2. Standardize Status codes.
 * 3. Deduplicate based on Tracking_ID (Prioritize New System).
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Logistics_Swamp;
CREATE FOLDER IF NOT EXISTS Logistics_Swamp.Shards;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Logistics_Swamp.Shards.LEGACY_SHP_A (
    TRACK_ID VARCHAR,
    DEST_CODE VARCHAR, -- 3 char
    STATUS_FULL VARCHAR, -- 'In-Transit', 'Delivered'
    LAST_UPD VARCHAR -- String Date
);

CREATE TABLE IF NOT EXISTS Logistics_Swamp.Shards.MODERN_SHP_B (
    T_ID VARCHAR,
    DESTINATION VARCHAR, -- Full Name
    ST_CODE VARCHAR, -- 'IT', 'DEL'
    UPD_TS TIMESTAMP
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Split, Overlap, Codes)
-------------------------------------------------------------------------------

-- Legacy Data
INSERT INTO Logistics_Swamp.Shards.LEGACY_SHP_A VALUES
('TRK-001', 'NYC', 'In-Transit', '2023-01-01'),
('TRK-002', 'LON', 'Delivered', '2023-01-05'),
('TRK-003', 'PAR', 'Lost', '2023-01-10');

-- Modern Data (Overlap on TRK-001)
INSERT INTO Logistics_Swamp.Shards.MODERN_SHP_B VALUES
('TRK-001', 'New York', 'IT', '2023-01-02 12:00:00'), -- Newer update
('TRK-004', 'Tokyo', 'IT', '2023-01-02 12:00:00'),
('TRK-005', 'Berlin', 'DEL', '2023-01-06 14:00:00');

-- Bulk Fill Legacy
INSERT INTO Logistics_Swamp.Shards.LEGACY_SHP_A VALUES
('TRK-006', 'LAX', 'In-Transit', '2023-01-01'),
('TRK-007', 'SFO', 'Pending', '2023-01-01'),
('TRK-008', 'MIA', 'Delivered', '2023-01-01'),
('TRK-009', 'DFW', 'In-Transit', '2023-01-01'),
('TRK-010', 'ORD', 'Delivered', '2023-01-01'),
('TRK-011', 'BOS', 'Pending', '2023-01-01'),
('TRK-012', 'SEA', 'In-Transit', '2023-01-01'),
('TRK-013', 'DEN', 'Delivered', '2023-01-01'),
('TRK-014', 'ATL', 'Pending', '2023-01-01'),
('TRK-015', 'PHX', 'In-Transit', '2023-01-01'),
('TRK-016', 'IAH', 'Delivered', '2023-01-01'),
('TRK-017', 'LAS', 'Pending', '2023-01-01'),
('TRK-018', 'MCO', 'In-Transit', '2023-01-01'),
('TRK-019', 'EWR', 'Delivered', '2023-01-01'),
('TRK-020', 'CLT', 'Pending', '2023-01-01'),
('TRK-021', 'MSP', 'In-Transit', '2023-01-01'),
('TRK-022', 'DTW', 'Delivered', '2023-01-01'),
('TRK-023', 'PHL', 'Pending', '2023-01-01'),
('TRK-024', 'BWI', 'In-Transit', '2023-01-01'),
('TRK-025', 'SLC', 'Delivered', '2023-01-01'),
('TRK-026', 'SAN', 'Pending', '2023-01-01'),
('TRK-027', 'IAD', 'In-Transit', '2023-01-01'),
('TRK-028', 'DCA', 'Delivered', '2023-01-01'),
('TRK-029', 'MDW', 'Pending', '2023-01-01'),
('TRK-030', 'HNL', 'In-Transit', '2023-01-01');

-- Bulk Fill Modern
INSERT INTO Logistics_Swamp.Shards.MODERN_SHP_B VALUES
('TRK-031', 'London', 'IT', '2023-01-01 10:00:00'),
('TRK-032', 'Paris', 'DEL', '2023-01-01 10:00:00'),
('TRK-033', 'Madrid', 'PND', '2023-01-01 10:00:00'),
('TRK-034', 'Rome', 'IT', '2023-01-01 10:00:00'),
('TRK-035', 'Berlin', 'DEL', '2023-01-01 10:00:00'),
('TRK-036', 'Vienna', 'PND', '2023-01-01 10:00:00'),
('TRK-037', 'Prague', 'IT', '2023-01-01 10:00:00'),
('TRK-038', 'Warsaw', 'DEL', '2023-01-01 10:00:00'),
('TRK-039', 'Moscow', 'PND', '2023-01-01 10:00:00'),
('TRK-040', 'Beijing', 'IT', '2023-01-01 10:00:00'),
('TRK-041', 'Shanghai', 'DEL', '2023-01-01 10:00:00'),
('TRK-042', 'Tokyo', 'PND', '2023-01-01 10:00:00'),
('TRK-043', 'Seoul', 'IT', '2023-01-01 10:00:00'),
('TRK-044', 'Mumbai', 'DEL', '2023-01-01 10:00:00'),
('TRK-045', 'Delhi', 'PND', '2023-01-01 10:00:00'),
('TRK-046', 'Dubai', 'IT', '2023-01-01 10:00:00'),
('TRK-047', 'Singapore', 'DEL', '2023-01-01 10:00:00'),
('TRK-048', 'Bangkok', 'PND', '2023-01-01 10:00:00'),
('TRK-049', 'Sydney', 'IT', '2023-01-01 10:00:00'),
('TRK-050', 'Melbourne', 'DEL', '2023-01-01 10:00:00'),
('TRK-006', 'Los Angeles', 'IT', '2023-01-02 10:00:00'), -- Overlap TRK-006
('TRK-007', 'San Francisco', 'DEL', '2023-01-02 10:00:00'); -- Overlap TRK-007

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Logistics_Swamp has data split between LEGACY_SHP_A and MODERN_SHP_B.
 *  Merge these into a single Single Source of Truth.
 *  
 *  1. Bronze: Wrap raw tables.
 *  2. Silver: 
 *     - UNION the two tables.
 *     - Map legacy status ('In-Transit') to modern codes ('IT').
 *     - Deduplicate: If a Tracking ID exists in both, keep the Modern record.
 *  3. Gold: 
 *     - Create a dashboard view counting Shipments by Status and Destination City.
 *  
 *  Generate the Dremio SQL."
 */
