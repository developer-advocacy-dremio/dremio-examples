/*
 * Dremio "Messy Data" Challenge: Gig Economy Driver Pay
 * 
 * Scenario: 
 * Driver earnings from rides.
 * 'Pay_Type' includes 'Base', 'Surge', 'Tip', 'Adjustment'.
 * Cancellations appear as small 'Fee' payments or negatives.
 * Data spread across 'Rides' and 'Adjustments' tables.
 * 
 * Objective for AI Agent:
 * 1. Union Data: Combine Rides and Adjustments.
 * 2. Calculate Net Pay: Sum(Amount) per Driver.
 * 3. Flag Low Earners: Avg Pay per Ride < Threshold.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Ride_App;
CREATE FOLDER IF NOT EXISTS Ride_App.Payroll;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Ride_App.Payroll.RIDES (
    RIDE_ID VARCHAR,
    DRIVER_ID VARCHAR,
    BASE_PAY DOUBLE,
    SURGE_MULT DOUBLE -- 1.0, 2.5
);

CREATE TABLE IF NOT EXISTS Ride_App.Payroll.ADJUSTMENTS (
    REF_ID VARCHAR, -- Links to Ride ID
    TYPE VARCHAR, -- 'Tip', 'Cancellation Fee', 'Refund'
    AMOUNT DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Surge, Tips)
-------------------------------------------------------------------------------

-- Ride
INSERT INTO Ride_App.Payroll.RIDES VALUES
('R-1', 'D-1', 10.0, 1.0),
('R-2', 'D-1', 10.0, 2.0); -- Surge 2x (Total should be 20)

-- Tips & Adjs
INSERT INTO Ride_App.Payroll.ADJUSTMENTS VALUES
('R-1', 'Tip', 5.0),
('R-2', 'Tip', 0.0);

-- Cancellation
INSERT INTO Ride_App.Payroll.RIDES VALUES
('R-3', 'D-2', 0.0, 1.0); -- Cancelled
INSERT INTO Ride_App.Payroll.ADJUSTMENTS VALUES
('R-3', 'Cancellation Fee', 5.0);

-- Refund (Negative)
INSERT INTO Ride_App.Payroll.RIDES VALUES
('R-4', 'D-3', 20.0, 1.0);
INSERT INTO Ride_App.Payroll.ADJUSTMENTS VALUES
('R-4', 'Refund', -20.0); -- Rider complaint

-- Bulk Fill
INSERT INTO Ride_App.Payroll.RIDES VALUES
('R-10', 'D-5', 10.0, 1.0),
('R-11', 'D-5', 10.0, 1.5),
('R-12', 'D-5', 10.0, 1.0),
('R-13', 'D-6', 50.0, 1.0), -- Long ride
('R-14', 'D-6', 5.0, 1.0),
('R-15', 'D-7', 0.0, 1.0),
('R-16', 'D-7', 0.0, 1.0),
('R-17', 'D-8', 100.0, 10.0), -- Huge Surge
('R-18', 'D-9', -10.0, 1.0), -- Error pay
('R-19', 'D-10', NULL, 1.0),
('R-20', 'D-10', 10.0, NULL),
('R-21', 'D-11', 12.50, 1.1),
('R-22', 'D-12', 15.00, 1.0),
('R-23', 'D-13', 8.00, 1.0),
('R-24', 'D-14', 9.00, 1.0),
('R-25', 'D-15', 10.00, 1.0),
('R-26', 'D-15', 10.00, 1.0), -- Dupe ID mismatch?
('R-27', 'D-16', 10.00, 0.0), -- Zero surge (Crash?)
('R-28', 'D-17', 10.00, -1.0), -- Negative surge
('R-29', 'D-18', 0.01, 1.0), -- Tiny pay
('R-30', 'D-19', 1000.0, 1.0); -- Whale ride

INSERT INTO Ride_App.Payroll.ADJUSTMENTS VALUES
('R-10', 'Tip', 1.0),
('R-11', 'Tip', 2.0),
('R-12', 'Gas', -5.0), -- Deduction
('R-13', 'Tip', 10.0),
('R-14', 'Bonus', 50.0);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Calculate driver pay in Ride_App.Payroll.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Calc Ride_Total: Base_Pay * Surge_Mult.
 *     - Flatten Adjustments: Pivot Tips and Fees by Ride_ID.
 *  3. Gold: 
 *     - Daily Earnings Report: Sum(Ride_Total + Tips + Fees) per Driver.
 *  
 *  Show the SQL."
 */
