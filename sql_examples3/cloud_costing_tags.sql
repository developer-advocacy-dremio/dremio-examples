/*
 * Dremio "Messy Data" Challenge: Cloud Costing Tags
 * 
 * Scenario: 
 * Multi-cloud billing data.
 * 3 Tables: RESOURCES (Meta), TAGS (KV Pairs), COST_LOGS (Usage).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: COST_LOGS -> RESOURCES -> TAGS.
 * 2. Normalize Tags: 'CostCenter', 'cost-center', 'CC' -> 'Cost_Center'.
 * 3. Calc Attribution: Sum Cost per Normalized Cost Center.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Cloud_FinOps;
CREATE FOLDER IF NOT EXISTS Cloud_FinOps.Billing;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Cloud_FinOps.Billing.RESOURCES (
    RESOURCE_ID VARCHAR,
    SERVICE_TYPE VARCHAR, -- 'EC2', 'S3'
    REGION VARCHAR
);

CREATE TABLE IF NOT EXISTS Cloud_FinOps.Billing.TAGS (
    TAG_ID VARCHAR,
    RESOURCE_ID VARCHAR,
    KEY_NAME VARCHAR,
    VALUE_STRING VARCHAR
);

CREATE TABLE IF NOT EXISTS Cloud_FinOps.Billing.COST_LOGS (
    LOG_ID VARCHAR,
    RESOURCE_ID VARCHAR,
    USAGE_DATE DATE,
    COST_USD DOUBLE,
    CURRENCY_CODE VARCHAR -- 'USD', 'EUR' (Mixed)
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- RESOURCES
INSERT INTO Cloud_FinOps.Billing.RESOURCES VALUES
('R-1', 'EC2', 'us-east-1'), ('R-2', 'S3', 'us-east-1'), ('R-3', 'RDS', 'us-west-2');

-- TAGS
INSERT INTO Cloud_FinOps.Billing.TAGS VALUES
('T-1', 'R-1', 'CostCenter', 'Marketing'),
('T-2', 'R-2', 'cost-center', 'Marketing'),
('T-3', 'R-3', 'CC', 'Eng'),
('T-4', 'R-1', 'Owner', 'Alice'),
('T-5', 'R-3', 'Project', 'X');

-- COST LOGS (50 Rows)
INSERT INTO Cloud_FinOps.Billing.COST_LOGS VALUES
('L-1', 'R-1', '2023-01-01', 10.0, 'USD'),
('L-2', 'R-1', '2023-01-02', 10.0, 'USD'),
('L-3', 'R-1', '2023-01-03', 10.0, 'USD'),
('L-4', 'R-1', '2023-01-04', 10.0, 'USD'),
('L-5', 'R-1', '2023-01-05', 10.0, 'USD'),
('L-6', 'R-1', '2023-01-06', 10.0, 'USD'),
('L-7', 'R-1', '2023-01-07', 10.0, 'USD'),
('L-8', 'R-1', '2023-01-08', 10.0, 'USD'),
('L-9', 'R-1', '2023-01-09', 10.0, 'USD'),
('L-10', 'R-1', '2023-01-10', 10.0, 'USD'),
('L-11', 'R-2', '2023-01-01', 5.0, 'USD'),
('L-12', 'R-2', '2023-01-02', 5.0, 'USD'),
('L-13', 'R-2', '2023-01-03', 5.0, 'USD'),
('L-14', 'R-2', '2023-01-04', 5.0, 'USD'),
('L-15', 'R-2', '2023-01-05', 5.0, 'USD'),
('L-16', 'R-2', '2023-01-06', 5.0, 'USD'),
('L-17', 'R-2', '2023-01-07', 5.0, 'USD'),
('L-18', 'R-2', '2023-01-08', 5.0, 'USD'),
('L-19', 'R-2', '2023-01-09', 5.0, 'USD'),
('L-20', 'R-2', '2023-01-10', 5.0, 'euro'), -- Mixed currency
('L-21', 'R-3', '2023-01-01', 100.0, 'USD'),
('L-22', 'R-3', '2023-01-02', 100.0, 'USD'),
('L-23', 'R-3', '2023-01-03', 100.0, 'USD'),
('L-24', 'R-3', '2023-01-04', 100.0, 'USD'),
('L-25', 'R-3', '2023-01-05', 100.0, 'USD'),
('L-26', 'R-3', '2023-01-06', 100.0, 'USD'),
('L-27', 'R-3', '2023-01-07', 100.0, 'USD'),
('L-28', 'R-3', '2023-01-08', 100.0, 'USD'),
('L-29', 'R-3', '2023-01-09', 100.0, 'USD'),
('L-30', 'R-3', '2023-01-10', 100.0, 'USD'),
('L-31', 'R-1', '2023-01-11', 10.0, 'USD'),
('L-32', 'R-1', '2023-01-12', 10.0, 'USD'),
('L-33', 'R-1', '2023-01-13', 10.0, 'USD'),
('L-34', 'R-1', '2023-01-14', 10.0, 'USD'),
('L-35', 'R-1', '2023-01-15', 10.0, 'USD'),
('L-36', 'R-1', '2023-01-16', 10.0, 'USD'),
('L-37', 'R-1', '2023-01-17', 10.0, 'USD'),
('L-38', 'R-1', '2023-01-18', 10.0, 'USD'),
('L-39', 'R-1', '2023-01-19', 10.0, 'USD'),
('L-40', 'R-1', '2023-01-20', 10.0, 'USD'),
('L-41', 'R-1', '2023-01-21', 10.0, 'USD'),
('L-42', 'R-1', '2023-01-22', 10.0, 'USD'),
('L-43', 'R-1', '2023-01-23', 10.0, 'USD'),
('L-44', 'R-2', '2023-01-11', 5.0, 'USD'),
('L-45', 'R-2', '2023-01-12', 5.0, 'USD'),
('L-46', 'R-2', '2023-01-13', 5.0, 'USD'),
('L-47', 'R-2', '2023-01-14', 5.0, 'USD'),
('L-48', 'R-2', '2023-01-15', 5.0, 'USD'),
('L-49', 'R-2', '2023-01-16', 5.0, 'USD'),
('L-50', 'R-2', '2023-01-17', 5.0, 'USD');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Allocate cloud costs in Cloud_FinOps.Billing.
 *  
 *  1. Bronze: Raw View of COST_LOGS, TAGS, RESOURCES.
 *  2. Silver: 
 *     - Pivot Tags: Create 'Cost_Center' column from normalized Key_Name.
 *     - Normalize Currency: Convert 'euro' to USD (~1.1x).
 *  3. Gold: 
 *     - Chargeback: Group by Cost_Center, Sum(Cost_USD).
 *  
 *  Show the SQL."
 */
