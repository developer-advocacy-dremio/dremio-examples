-------------------------------------------------------------------------------
-- Dremio SQL Admin Examples: Access Control & RBAC
-- 
-- This script demonstrates comprehensive User, Role, and Privilege management.
-- Scenarios include:
-- 1. User Lifecycle (Create, Alter, Drop)
-- 2. Role Hierarchies
-- 3. Fine-Grained Access Control (Tables, Rows, Columns)
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. USER MANAGEMENT
-------------------------------------------------------------------------------

-- 1.1 Create a new local user
-- Note: Requires ADMIN privilege. Password complexity rules apply.
CREATE USER "analyst_dave" PASSWORD 'SecurePass123!';

-- 1.2 Alter user password
ALTER USER "analyst_dave" SET PASSWORD 'NewSecurePass456!';

-- 1.3 Drop a user
-- DROP USER "analyst_dave"; -- Commented out to prevent accidental execution

-------------------------------------------------------------------------------
-- 2. ROLE MANAGEMENT (RBAC)
-------------------------------------------------------------------------------

-- 2.1 Create Roles for functional groups
CREATE ROLE "DataAnalysts";
CREATE ROLE "EngineeringLeads";
CREATE ROLE "FinanceViewers";

-- 2.2 Assign Roles to Users
GRANT ROLE "DataAnalysts" TO USER "analyst_dave";

-- 2.3 Role Hierarchies (Nested Roles)
-- Granting 'FinanceViewers' to 'DataAnalysts' means Analysts inherit Finance permissions.
GRANT ROLE "FinanceViewers" TO ROLE "DataAnalysts";

-- 2.4 Verify Role Grants
-- (System table usage - Dremio Enterprise feature)
-- SELECT * FROM sys.roles;
-- SELECT * FROM sys.memberships;

-------------------------------------------------------------------------------
-- 3. PRIVILEGE MANAGEMENT (Basic)
-------------------------------------------------------------------------------

-- 3.1 Source Level Access
-- Allow EngineeringLeads to create tables in the Nessie catalog.
GRANT CREATE TABLE ON SOURCE "NessieCatalog" TO ROLE "EngineeringLeads";

-- 3.2 Folder Level Access
-- Allow Analysts to edit datasets in a specific folder.
GRANT ALTER ON FOLDER "DataLake". "Analysis_Projects" TO ROLE "DataAnalysts";

-- 3.3 Dataset Access (View/Table)
-- Read-only access for Finance.
GRANT SELECT ON TABLE "FinanceDB"."Gold"."Revenue_Report" TO ROLE "FinanceViewers";

-------------------------------------------------------------------------------
-- 4. ADVANCED ACCESS CONTROL
-------------------------------------------------------------------------------

-- 4.1 Ownership Transfer
-- Move ownership of a critical VDS to a stable role rather than an individual user.
ALTER TABLE "DataLake"."Refined"."Customer_360" OWNER TO ROLE "EngineeringLeads";

-- 4.2 Revoking Privileges
-- Remove creation rights if role changes scope.
REVOKE CREATE TABLE ON SOURCE "NessieCatalog" FROM ROLE "EngineeringLeads";

-- 4.3 Row-Level & Column-Level Access (Concept)
-- Dremio handles this via SQL UDFs and Views, or via Ranger integration (if applicable).
-- Example: Creating a view that filters rows based on the current user.

-- CREATE VIEW "HR"."Secure_Salaries" AS
-- SELECT Name, Dept, 
--   CASE WHEN IS_MEMBER('HR_Managers') THEN Salary ELSE NULL END AS Salary
-- FROM "HR"."Employees"
-- WHERE Dept = 'Sales' OR IS_MEMBER('HR_Managers');

-------------------------------------------------------------------------------
-- 5. CLEANUP (For Demo Purposes)
-------------------------------------------------------------------------------
-- DROP ROLE "DataAnalysts";
-- DROP ROLE "EngineeringLeads";
-- DROP USER "analyst_dave";
