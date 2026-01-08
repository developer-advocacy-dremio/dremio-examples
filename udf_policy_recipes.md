# Dremio UDF Policy Recipes

This guide demonstrates how to implement scalable, reusable security policies in Dremio using User Defined Functions (UDFs). Unlike static Views, UDF-based policies are dynamic and can be applied to multiple tables.

## 1. Basics: User Defined Functions (UDFs)

UDFs encapsulate logic that can return a value (Scalar) or a table (Tabular). For security policies, we primarily use Scalar UDFs that return a BOOLEAN (for Row Access) or a value (for Column Masking).

### Syntax
```sql
CREATE FUNCTION <function_name> ( <param_name> <type> [, ...] )
RETURNS <return_type>
RETURN <expression>;
```

### Context Functions
These built-in functions allow your UDF to know *who* is running the query:
- `USER`: Returns the username of the current user.
- `IS_MEMBER('RoleName')`: Returns `TRUE` if the user is in the specified role (or nested role).
- `query_user()`: (Cloud/Enterprise) Returns the user running the query operation.

---

## 2. Row Access Policies (RAP)

Row Access Policies act as an automatic `WHERE` clause appended to every query against the table. If the UDF returns `TRUE`, the row is visible.

### Syntax
```sql
ALTER TABLE <table_name> ADD ROW ACCESS POLICY <policy_function>(<args>);
```

### Recipe 1: Region-Based Filtering
**Goal**: Users in the 'US_Team' role see US rows; 'EU_Team' see EU rows.
**Setup**: Table `Sales.Transactions` has a column `Region`.

**Step 1: Create the Policy Function**
```sql
CREATE FUNCTION Policy.CanViewRegion (region_val VARCHAR)
RETURNS BOOLEAN
RETURN 
  (region_val = 'US' AND IS_MEMBER('US_Team'))
  OR 
  (region_val = 'EU' AND IS_MEMBER('EU_Team'))
  OR
  IS_MEMBER('Global_Admins'); -- Admins see everything
```

**Step 2: Apply to Table**
```sql
ALTER TABLE Sales.Transactions 
ADD ROW ACCESS POLICY Policy.CanViewRegion(Region);
```

### Recipe 2: Hierarchy / Ownership Filtering
**Goal**: Managers see all rows; Employees only see rows where they are the 'Owner'.

**Step 1: Create Policy Function**
```sql
CREATE FUNCTION Policy.IsOwnerOrManager (owner_name VARCHAR)
RETURNS BOOLEAN
RETURN 
  IS_MEMBER('Managers') -- Manager sees all
  OR 
  owner_name = USER;    -- Employee sees their own rows
```

**Step 2: Apply to Table**
```sql
ALTER TABLE HR.PerformanceReviews
ADD ROW ACCESS POLICY Policy.IsOwnerOrManager(EmployeeName);
```

---

## 3. Column Masking Policies (CMP)

Column Masking Policies act as an automatic `CASE` statement wrapping the column on read.

### Syntax
```sql
ALTER TABLE <table_name> MODIFY COLUMN <col_name> SET MASKING POLICY <policy_function>(<args>);
```

### Recipe 3: Dynamic PII Redaction
**Goal**: Show full Email to 'Marketing' role; redact domain for others.

**Step 1: Create Masking Function**
```sql
CREATE FUNCTION Policy.MaskEmail (email_val VARCHAR)
RETURNS VARCHAR
RETURN 
  CASE 
    WHEN IS_MEMBER('Marketing') THEN email_val
    ELSE REGEXP_REPLACE(email_val, '@.*', '@****.com') 
  END;
```

**Step 2: Apply to Table**
```sql
ALTER TABLE CRM.Contacts 
MODIFY COLUMN EmailAddress SET MASKING POLICY Policy.MaskEmail(EmailAddress);
```

### Recipe 4: Conditional Nulling (Salary)
**Goal**: Only 'HR' sees salary; others see NULL. NOTE: Return type must match column type.

**Step 1: Create Masking Function**
```sql
CREATE FUNCTION Policy.MaskSalary (salary_val DOUBLE)
RETURNS DOUBLE
RETURN 
  CASE 
    WHEN IS_MEMBER('HR') THEN salary_val
    ELSE NULL
  END;
```

**Step 2: Apply to Table**
```sql
ALTER TABLE HR.Employees
MODIFY COLUMN AnnualSalary SET MASKING POLICY Policy.MaskSalary(AnnualSalary);
```

---

## 4. Policy Management

### Viewing Policies
You can usually verify policies by inspecting the `sys.tables` or dedicated system views (depending on Dremio version).
```sql
-- Check documentation for specific system table queries
-- Often visible in the UI under Table Settings -> Privileges/Governance
```

### Dropping Policies
To remove a restriction:

```sql
-- Remove Row Access Policy
ALTER TABLE Sales.Transactions DROP ROW ACCESS POLICY;

-- Remove Column Masking Policy
ALTER TABLE CRM.Contacts MODIFY COLUMN EmailAddress UNSET MASKING POLICY;
```
