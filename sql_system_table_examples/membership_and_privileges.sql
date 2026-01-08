-------------------------------------------------------------------------------
-- Dremio SQL Examples: System Tables - Membership & Privileges
-- 
-- This script demonstrates auditing Users, Roles, and Access Control.
-- Note: Availability of these tables depends on Dremio Edition (Enterprise/Cloud).
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. USER INVENTORY
-- List all active users and their creation details.
-------------------------------------------------------------------------------

SELECT 
    user_name,
    first_name,
    last_name,
    email,
    created_at,
    active
FROM sys.users
ORDER BY created_at DESC;

-------------------------------------------------------------------------------
-- 2. ROLE MEMBERSHIP
-- Flatten the hierarchy to see which users belong to which roles.
-------------------------------------------------------------------------------

SELECT 
    role_name,
    member_name,
    member_type -- USER or ROLE
FROM sys.memberships
ORDER BY role_name, member_name;

-------------------------------------------------------------------------------
-- 3. AUDITING PRIVILEGES (If sys.privileges is available)
-- Check who has 'GRANT' access on specific objects.
-------------------------------------------------------------------------------

/*
SELECT 
    grantee_type,
    grantee_name,
    privilege,
    object_type,
    object_name
FROM sys.privileges
WHERE object_name LIKE '%SensitiveData%'
ORDER BY object_name;
*/

-------------------------------------------------------------------------------
-- 4. ROLE ASSIGNMENT CHECK
-- Find users who do not have a specific role (e.g., ensuring everyone is in 'AllUsers')
-------------------------------------------------------------------------------

SELECT user_name 
FROM sys.users
WHERE user_name NOT IN (
    SELECT member_name 
    FROM sys.memberships 
    WHERE role_name = 'AllUsers'
);
