# Dremio Data Masking Recipes

This guide provides practical SQL recipes for implementing Data Masking and Row-Level Security (RLS) in Dremio. These patterns help secure PII (Personally Identifiable Information) while maintaining data utility for analysts.

## 1. Column-Level Masking (Dynamic)

Dynamic masking hides data in specific columns based on the user's role, without changing the underlying data.

### Recipe 1: Full Redaction for Non-Privileged Users
**Scenario**: Only users in the `HR_Managers` role can see the `salary` column. Everyone else sees `NULL` or `-1`.

```sql
CREATE VIEW HR.Secure_Employees AS
SELECT
  EmployeeID,
  Name,
  CASE 
    WHEN IS_MEMBER('HR_Managers') THEN Salary 
    ELSE NULL 
  END AS Salary
FROM Source.HR.Employees;
```

### Recipe 2: Partial Masking (Email / Phone)
**Scenario**: Analysts need to see unique identifiers for joining but shouldn't see full emails.
**Function**: `MASK_SHOW_LAST_N(input, n, upper_char, lower_char, digit_char)`

```sql
SELECT
  UserID,
  -- Result: xxxx@dremio.com
  REGEXP_REPLACE(Email, '^[^@]+', 'xxxx') AS Masked_Email,
  
  -- Result: ###-###-1234
  MASK_SHOW_LAST_N(PhoneNumber, 4, '#', '#', '#') AS Masked_Phone
FROM Customers;
```

### Recipe 3: Hash Masking (Pseudonymization)
**Scenario**: Data needs to be joinable across tables (e.g., for Churn Analysis) but the actual `UserID` must remain secret.
**Function**: `SHA256` or `MD5`

```sql
SELECT
  -- Deterministic hash: Same input always produces same output
  TO_HEX(SHA256(CAST(SocialSecurityNum AS VARBINARY))) AS Hashed_SSN,
  Region,
  CreditScore
FROM Finance.Clients;
```

## 2. Row-Level Security (RLS)

RLS restricts which *rows* a user can see based on their identity.

### Recipe 4: Region-Based Access
**Scenario**: US employees see US data; EU employees see EU data.

```sql
CREATE VIEW Sales.Regional_Data AS
SELECT * 
FROM Sales.Transactions
WHERE 
  (Region = 'US' AND IS_MEMBER('US_Team'))
  OR 
  (Region = 'EU' AND IS_MEMBER('EU_Team'))
  OR
  IS_MEMBER('Global_Admins'); -- Admins see everything
```

### Recipe 5: Parameterized Lookup Table
**Scenario**: Access logic is complex and stored in a mapping table `Security.AccessMap` (User -> AllowedRegion).

```sql
CREATE VIEW Sales.Secure_Transactions AS
SELECT t.*
FROM Sales.Transactions t
JOIN Security.AccessMap m ON t.Region = m.AllowedRegion
WHERE m.UserName = USER; -- Built-in 'USER' returns current username
```

## 3. Compliance Utilities

### Recipe 6: Identifying PII Columns (Regex)
**Scenario**: Quickly scan a table to flag columns that might contain emails.

```sql
SELECT 
  COUNT(*) FILTER (WHERE REGEXP_LIKE(ColumnA, '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')) AS Potential_Emails
FROM Raw.IncomingData;
```

### Recipe 7: "Right to be Forgotten" (GDPR)
**Scenario**: Filter out users who have requested deletion, using an Anti-Join.

```sql
CREATE VIEW Marketing.Active_Contacts AS
SELECT c.*
FROM CRM.Contacts c
LEFT JOIN Legal.DeletionRequests d ON c.UserID = d.UserID
WHERE d.UserID IS NULL;
```
