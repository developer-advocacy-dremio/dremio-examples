# Dremio Privilege Management Recipes

This guide provides a comprehensive reference for granting privileges across the Dremio object hierarchy. It covers System, Container, Folder, and Dataset level permissions.

## 1. System Privileges
These privileges apply to the entire Dremio instance or organization.

### Admin Access
**Goal**: Grant full administrative control to a user or role.
```sql
GRANT ADMIN TO ROLE "SysAdmins";
GRANT ADMIN TO USER "manager@dremio.com";
```

### Source Management
**Goal**: Allow a team to connect new data sources.
```sql
GRANT CREATE SOURCE ON SYSTEM TO ROLE "DataEngineers";
```

### User/Role Management
**Goal**: Allow HR to manage users without full Admin rights (if supported by edition).
```sql
GRANT CREATE USER ON SYSTEM TO ROLE "HR_Systems";
GRANT CREATE ROLE ON SYSTEM TO ROLE "HR_Systems";
```

---

## 2. Container Privileges (Catalogs, Sources, Spaces)
Containers are top-level objects that hold datasets.

### Sources (e.g., S3, Nessie, Postgres)
**Goal**: Allow engineers to create tables/PDS in a source.
```sql
-- Allow creating tables (formatting folders as PDS)
GRANT CREATE TABLE ON SOURCE "S3_DataLake" TO ROLE "DataEngineers";

-- Allow drop/alter on the source configuration itself
GRANT MODIFY ON SOURCE "S3_DataLake" TO ROLE "LeadEngineers";
```

### Spaces (Semantic Layer Roots)
**Goal**: Allow analysts to create views and folders in a specific space.
```sql
-- Allow creating new views/folders
GRANT CREATE VIEW ON SPACE "Marketing" TO ROLE "MarketingAnalysts";
GRANT CREATE FOLDER ON SPACE "Marketing" TO ROLE "MarketingAnalysts";

-- Allow changing space properties
GRANT ALTER ON SPACE "Marketing" TO ROLE "MarketingLeads";
```

---

## 3. Folder Privileges
Privileges on folders are inherited by the datasets and sub-folders within them.

### Permission Inheritance
**Goal**: Give read access to everything inside the "QuarterlyReports" folder.
```sql
-- 'SELECT' on a folder grants SELECT on all current and future datasets inside
GRANT SELECT ON FOLDER "Marketing"."QuarterlyReports" TO ROLE "Executives";
```

### Managing Content
**Goal**: Allow a team to organize a specific folder hierarchy.
```sql
-- Allow renaming/moving/deleting the folder and its contents
GRANT ALTER ON FOLDER "DataScience"."Sandbox" TO ROLE "DataScientist";
GRANT DROP ON FOLDER "DataScience"."Sandbox" TO ROLE "DataScientist";
```

---

## 4. Dataset Privileges (Tables & Views)
Fine-grained control over specific datasets.

### Read Access
**Goal**: Standard read-only access.
```sql
GRANT SELECT ON TABLE "Sales"."Transactions" TO ROLE "SalesTeam";
GRANT SELECT ON VIEW "Marketing"."CampaignPerformance" TO ROLE "MarketingTeam";
```

### Write/Manage Access
**Goal**: Allow table owners or ETL jobs to update data or schema.
```sql
-- Allow INSERT/UPDATE/DELETE (for Iceberg tables)
GRANT INSERT, UPDATE, DELETE ON TABLE "DataLake"."Iceberg"."Orders" TO ROLE "ETL_ServiceAccount";

-- Allow altering schema (add columns)
GRANT ALTER ON TABLE "DataLake"."Iceberg"."Orders" TO ROLE "DataArchitects";
```

---

## 5. Function Privileges (UDFs)
Control who can execute specific logic.

**Goal**: Allow specific roles to use a sensitive masking function.
```sql
GRANT EXECUTE ON FUNCTION "Security"."MaskSSN" TO ROLE "HR_Managers";
```

---

## 6. Ownership
Ownership implies full control (ALL PRIVILEGES). Ownership can be transferred.

**Goal**: Transfer ownership of a critical view to a team role to prevent loss of access if a user leaves.
```sql
ALTER TABLE "Finance"."RevenueReport" OWNER TO ROLE "FinanceLeads";
```

---

## 7. Revoking Privileges
The `REVOKE` syntax mirrors `GRANT`.

```sql
-- Remove read access
REVOKE SELECT ON TABLE "Sales"."Transactions" FROM ROLE "Interns";

-- Remove admin access
REVOKE ADMIN FROM USER "contractor@example.com";
```
