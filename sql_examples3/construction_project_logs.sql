/*
 * Dremio "Messy Data" Challenge: Construction Project Logs
 * 
 * Scenario: 
 * Project Tasks with dependencies ('Predecessor_ID').
 * Budget is a text field ('$100k', '500.00').
 * 'Status' is inconsistent ('Done', 'Complete', 'In Progress', 'WIP').
 * 
 * Objective for AI Agent:
 * 1. Clean Budget to Numeric.
 * 2. Standardize Status to 'Not Started', 'In Progress', 'Completed'.
 * 3. Detect Circular Dependencies or logic errors (Start Date > End Date).
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS City_Builder;
CREATE FOLDER IF NOT EXISTS City_Builder.Gantt_Chart;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS City_Builder.Gantt_Chart.TASKS (
    TASK_ID VARCHAR,
    PRED_ID VARCHAR, -- Predecessor
    TASK_NAME VARCHAR,
    START_DT DATE,
    END_DT DATE,
    BUDGET_TXT VARCHAR,
    STATUS_RAW VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Cycles, Text Fields)
-------------------------------------------------------------------------------

-- Standard Sequence
INSERT INTO City_Builder.Gantt_Chart.TASKS VALUES
('T-1', NULL, 'Excavation', '2023-01-01', '2023-01-10', '$10,000', 'Done'),
('T-2', 'T-1', 'Foundation', '2023-01-11', '2023-01-20', '20000', 'Complete');

-- Messy Status / Budget
INSERT INTO City_Builder.Gantt_Chart.TASKS VALUES
('T-3', 'T-2', 'Framing', '2023-01-21', '2023-02-10', '50k', 'WIP'),
('T-4', 'T-3', 'Roofing', '2023-02-11', '2023-02-20', '$15.5K', 'In Progress');

-- Logic Errors
INSERT INTO City_Builder.Gantt_Chart.TASKS VALUES
('T-5', 'T-4', 'Wiring', '2023-03-01', '2023-02-01', '5000', 'Pending'); -- End before Start

-- Bulk Fill
INSERT INTO City_Builder.Gantt_Chart.TASKS VALUES
('T-10', NULL, 'Site Prep', '2023-01-01', '2023-01-05', '1000', 'Done'),
('T-11', 'T-10', 'Paving', '2023-01-06', '2023-01-10', '$2,000.00', 'Finished'),
('T-12', 'T-11', 'Painting', '2023-01-11', '2023-01-15', '500', 'Done'),
('T-13', 'T-12', 'Cleanup', '2023-01-16', '2023-01-20', '100', 'Closed'),
('T-20', 'T-21', 'Cycle A', '2023-01-01', '2023-01-10', '100', 'WIP'),
('T-21', 'T-20', 'Cycle B', '2023-01-11', '2023-01-20', '100', 'WIP'), -- Circular Dependency (A needs B, B needs A)
('T-30', NULL, 'Test', '2023-01-01', '2023-01-01', '0', 'Start'),
('T-31', NULL, 'Test', '2023-01-01', '2023-01-01', 'Free', 'End'),
('T-32', NULL, 'Test', '2023-01-01', '2023-01-01', 'Unknown', 'Doing'),
('T-40', NULL, 'Phase 1', '2023-01-01', '2023-01-31', '1M', 'Done'),
('T-41', 'T-40', 'Phase 2', '2023-02-01', '2023-02-28', '2.5M', 'WIP'),
('T-42', 'T-41', 'Phase 3', '2023-03-01', '2023-03-31', '500K', 'Pending'),
('T-50', NULL, 'Safety Check', '2023-01-01', '2023-01-01', 'N/A', 'Passed'),
('T-51', NULL, 'Inspection', '2023-01-01', '2023-01-01', '-', 'Failed'),
('T-60', NULL, 'Dupe Task', '2023-01-01', '2023-01-02', '100', 'Done'),
('T-60', NULL, 'Dupe Task', '2023-01-01', '2023-01-02', '100', 'Done');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Review the Gantt chart in City_Builder.Gantt_Chart.TASKS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Standardize Status: Map 'Done','Complete','Finished','Closed' -> 'Completed'. Map 'WIP','Doing' -> 'In Progress'.
 *     - Clean Budget: Handle 'K', 'M', '$', ',' to create 'Cost_Numeric'.
 *     - Calc Duration: Days between Start and End.
 *  3. Gold: 
 *     - Summarize Total Cost by Status.
 *     - List Tasks where End_Date < Start_Date (Data Errors).
 *  
 *  Show the SQL."
 */
