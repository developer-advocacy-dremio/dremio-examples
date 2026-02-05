/*
 * Dremio "Messy Data" Challenge: Restaurant POS Variations
 * 
 * Scenario: 
 * Sales data from different franchise locations with unmanaged menu entries.
 * 'Item_Name' has variations: 'Burger', 'Chz Burger', 'Burger - No Cheese'.
 * 'Tax_Amt' is sometimes 0 (Cash sales?), sometimes wrong.
 * 
 * Objective for AI Agent:
 * 1. Categorize Items: Group 'Burger' variants under 'Main', 'Coke' under 'Drink'.
 * 2. Audit Tax: Check if Tax is consistent (e.g. ~8% of Net Sales).
 * 3. Daily Sales Report.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Fast_Food_Inc;
CREATE FOLDER IF NOT EXISTS Fast_Food_Inc.Sales_Logs;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Fast_Food_Inc.Sales_Logs.TRANSACTIONS (
    TX_ID VARCHAR,
    LOC_ID VARCHAR,
    ITEM_NAME VARCHAR,
    NET_AMT DOUBLE,
    TAX_AMT DOUBLE, -- Should be ~8%
    TS TIMESTAMP
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Menu Chaos)
-------------------------------------------------------------------------------

-- Burgers
INSERT INTO Fast_Food_Inc.Sales_Logs.TRANSACTIONS VALUES
('T-1', 'L-1', 'Cheeseburger', 5.00, 0.40, '2023-01-01 12:00:00'),
('T-2', 'L-2', 'Chz Brgr', 5.00, 0.40, '2023-01-01 12:05:00'),
('T-3', 'L-1', 'Burger - No Cheese', 4.50, 0.36, '2023-01-01 12:10:00'),
('T-4', 'L-3', 'Hamburger', 4.50, 0.36, '2023-01-01 12:15:00');

-- Drinks
INSERT INTO Fast_Food_Inc.Sales_Logs.TRANSACTIONS VALUES
('T-5', 'L-1', 'Coke', 2.00, 0.16, '2023-01-01 12:20:00'),
('T-6', 'L-2', 'Lg Soda', 2.50, 0.20, '2023-01-01 12:25:00'),
('T-7', 'L-3', 'Pepsi', 2.00, 0.16, '2023-01-01 12:30:00');

-- Tax Errors
INSERT INTO Fast_Food_Inc.Sales_Logs.TRANSACTIONS VALUES
('T-8', 'L-1', 'Fries', 3.00, 0.00, '2023-01-01 12:35:00'), -- Missing Tax
('T-9', 'L-2', 'Salad', 6.00, 10.00, '2023-01-01 12:40:00'); -- Tax > Net?

-- Bulk Fill
INSERT INTO Fast_Food_Inc.Sales_Logs.TRANSACTIONS VALUES
('T-10', 'L-1', 'Coke Zero', 2.00, 0.16, '2023-01-01 13:00:00'),
('T-11', 'L-1', 'Diet Coke', 2.00, 0.16, '2023-01-01 13:05:00'),
('T-12', 'L-1', 'Sprite', 2.00, 0.16, '2023-01-01 13:10:00'),
('T-13', 'L-1', 'Water', 0.00, 0.00, '2023-01-01 13:15:00'), -- Free
('T-14', 'L-1', 'Combo 1', 10.00, 0.80, '2023-01-01 13:20:00'),
('T-15', 'L-1', 'Combo #1', 10.00, 0.80, '2023-01-01 13:25:00'),
('T-16', 'L-1', 'Kid Meal', 5.00, 0.40, '2023-01-01 13:30:00'),
('T-17', 'L-1', 'Kids Meal', 5.00, 0.40, '2023-01-01 13:35:00'),
('T-18', 'L-2', 'Shake', 4.00, 0.32, '2023-01-01 13:40:00'),
('T-19', 'L-2', 'Choc Shake', 4.00, 0.32, '2023-01-01 13:45:00'),
('T-20', 'L-2', 'Van Shake', 4.00, 0.32, '2023-01-01 13:50:00'),
('T-21', 'L-3', 'Chicken Sand', 6.00, 0.48, '2023-01-01 14:00:00'),
('T-22', 'L-3', 'Chk Sand', 6.00, 0.48, '2023-01-01 14:05:00'),
('T-23', 'L-3', 'Spicy Chk', 6.50, 0.52, '2023-01-01 14:10:00'),
('T-24', 'L-3', 'Nuggets (6)', 3.00, 0.24, '2023-01-01 14:15:00'),
('T-25', 'L-3', 'Nuggets (10)', 5.00, 0.40, '2023-01-01 14:20:00'),
('T-26', 'L-1', 'Refund', -5.00, -0.40, '2023-01-01 15:00:00'),
('T-27', 'L-1', 'Void', 0.00, 0.00, '2023-01-01 15:05:00'),
('T-28', 'L-1', 'Cookie', 1.00, 0.08, '2023-01-01 15:10:00'),
('T-29', 'L-1', 'Cookie', 1.00, 0.08, '2023-01-01 15:15:00'),
('T-30', 'L-2', 'Coffee', 1.50, 0.12, '2023-01-01 08:00:00'),
('T-31', 'L-2', 'Decaf', 1.50, 0.12, '2023-01-01 08:05:00'),
('T-32', 'L-2', 'Hot Tea', 1.50, 0.12, '2023-01-01 08:10:00'),
('T-33', 'L-3', 'Iced Tea', 2.00, 0.16, '2023-01-01 12:00:00'),
('T-34', 'L-3', 'Sweet Tea', 2.00, 0.16, '2023-01-01 12:05:00'),
('T-35', 'L-3', 'Lemonade', 2.50, 0.20, '2023-01-01 12:10:00'),
('T-36', 'L-1', 'Apple Pie', 1.50, 0.12, '2023-01-01 16:00:00'),
('T-37', 'L-1', 'Cherry Pie', 1.50, 0.12, '2023-01-01 16:05:00'),
('T-38', 'L-1', 'Manager Meal', 0.00, 0.00, '2023-01-01 17:00:00'),
('T-39', 'L-1', 'Promo', 0.00, 0.00, '2023-01-01 17:05:00'),
('T-40', 'L-1', 'Gift Card', 20.00, 0.00, '2023-01-01 18:00:00'); -- No Tax on GC

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze POS data in Fast_Food_Inc.Sales_Logs.TRANSACTIONS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Standardize Categories: 
 *       CASE 
 *         WHEN LOWER(ITEM_NAME) LIKE '%burger%' THEN 'Burger'
 *         WHEN LOWER(ITEM_NAME) LIKE '%coke%' OR LOWER(ITEM_NAME) LIKE '%drink%' OR LOWER(ITEM_NAME) LIKE '%tea%' THEN 'Drink'
 *         ELSE 'Other'
 *       END.
 *     - Calc 'Variance': Abs(TAX_AMT - (NET_AMT * 0.08)).
 *  3. Gold: 
 *     - Report Total Net Sales and Total Tax Variance by Location (LOC_ID).
 *  
 *  Show the SQL."
 */
