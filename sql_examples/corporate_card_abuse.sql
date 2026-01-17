/*
 * Corporate Card Abuse Detection Demo
 * 
 * Scenario:
 * Internal Audit monitors corporate credit card usage for policy violations.
 * Red flags: Weekend spending, Restricted Merchants (Casinos, Liquor Stores), Split Transactions.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Report potential misuse by Department.
 * 
 * Note: Assumes a catalog named 'ExpenseAuditDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ExpenseAuditDB;
CREATE FOLDER IF NOT EXISTS ExpenseAuditDB.Bronze;
CREATE FOLDER IF NOT EXISTS ExpenseAuditDB.Silver;
CREATE FOLDER IF NOT EXISTS ExpenseAuditDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Transaction Logs
-------------------------------------------------------------------------------
-- Description: Raw card swipe data and category codes.

CREATE TABLE IF NOT EXISTS ExpenseAuditDB.Bronze.ExpenseTxns (
    TxnID VARCHAR,
    EmployeeID VARCHAR,
    Department VARCHAR,
    TxnDate DATE,
    Amount DOUBLE,
    MerchantName VARCHAR,
    MCC_Code INT -- Merchant Category Code
);

CREATE TABLE IF NOT EXISTS ExpenseAuditDB.Bronze.MCC_Codes (
    Code INT,
    Category VARCHAR,
    IsRestricted BOOLEAN
);

-- 1.1 Populate ExpenseTxns (50+ Records)
INSERT INTO ExpenseAuditDB.Bronze.ExpenseTxns (TxnID, EmployeeID, Department, TxnDate, Amount, MerchantName, MCC_Code) VALUES
('T-001', 'E-100', 'Sales', '2025-01-10', 150.00, 'Hilton', 3501),
('T-002', 'E-100', 'Sales', '2025-01-11', 200.00, 'Steakhouse', 5812), -- Weekend (Jan 11 is Sat)
('T-003', 'E-101', 'IT', '2025-01-13', 1200.00, 'Dell', 5732),
('T-004', 'E-102', 'HR', '2025-01-13', 50.00, 'Staples', 5943),
('T-005', 'E-103', 'Sales', '2025-01-14', 500.00, 'Delta', 4511),
('T-006', 'E-104', 'Marketing', '2025-01-15', 3000.00, 'Google Ads', 7311),
('T-007', 'E-100', 'Sales', '2025-01-12', 100.00, 'Casino Royale', 7995), -- Restricted (Sunday)
('T-008', 'E-105', 'IT', '2025-01-15', 15.00, 'Uber', 4121),
('T-009', 'E-106', 'Finance', '2025-01-16', 25.00, 'Starbucks', 5814),
('T-010', 'E-100', 'Sales', '2025-01-17', 150.00, 'Marriott', 3504),
('T-011', 'E-107', 'Sales', '2025-01-13', 400.00, 'Hertz', 3351),
('T-012', 'E-108', 'IT', '2025-01-14', 2000.00, 'AWS', 7372),
('T-013', 'E-109', 'HR', '2025-01-15', 75.00, 'Dunkin', 5814),
('T-014', 'E-110', 'Sales', '2025-01-16', 60.00, 'Taxi', 4121),
('T-015', 'E-111', 'Marketing', '2025-01-17', 250.00, 'Facebook Ads', 7311),
('T-016', 'E-112', 'Finance', '2025-01-18', 500.00, 'Golf Club', 7992), -- Weekend + Restricted?
('T-017', 'E-113', 'Sales', '2025-01-19', 300.00, 'Liquor Barn', 5921), -- Restricted
('T-018', 'E-114', 'IT', '2025-01-20', 100.00, 'Best Buy', 5732),
('T-019', 'E-115', 'HR', '2025-01-20', 45.00, 'Pizza Hut', 5814),
('T-020', 'E-116', 'Sales', '2025-01-21', 120.00, 'United', 4511),
('T-021', 'E-117', 'Marketing', '2025-01-22', 180.00, 'Adobe', 7372),
('T-022', 'E-118', 'Finance', '2025-01-23', 90.00, 'Office Depot', 5943),
('T-023', 'E-119', 'Sales', '2025-01-24', 220.00, 'Hyatt', 3501),
('T-024', 'E-120', 'IT', '2025-01-25', 1000.00, 'Newegg', 5732), -- Weekend
('T-025', 'E-121', 'HR', '2025-01-26', 30.00, 'Subway', 5814), -- Weekend
('T-026', 'E-122', 'Sales', '2025-01-27', 40.00, 'Lyft', 4121),
('T-027', 'E-123', 'Marketing', '2025-01-28', 500.00, 'LinkedIn', 7311),
('T-028', 'E-124', 'Finance', '2025-01-29', 200.00, 'Audit Firm', 8931),
('T-029', 'E-125', 'Sales', '2025-01-30', 130.00, 'Avis', 3351),
('T-030', 'E-126', 'IT', '2025-01-31', 600.00, 'Microsoft', 7372),
('T-031', 'E-127', 'HR', '2025-02-01', 50.00, 'Florist', 5992), -- Weekend
('T-032', 'E-128', 'Sales', '2025-02-02', 200.00, 'Ticketmaster', 7922), -- Weekend + Restricted
('T-033', 'E-129', 'Marketing', '2025-02-03', 150.00, 'Print Shop', 7333),
('T-034', 'E-130', 'Finance', '2025-02-04', 300.00, 'LegalZoom', 8111),
('T-035', 'E-131', 'Sales', '2025-02-05', 80.00, 'SFO Parsing', 7523),
('T-036', 'E-132', 'IT', '2025-02-06', 450.00, 'Apple Store', 5732),
('T-037', 'E-133', 'HR', '2025-02-07', 100.00, 'Catering', 5812),
('T-038', 'E-134', 'Sales', '2025-02-08', 350.00, 'Spa Resort', 7298), -- Restricted + Weekend
('T-039', 'E-135', 'Marketing', '2025-02-09', 20.00, 'McDonalds', 5814),
('T-040', 'E-136', 'Finance', '2025-02-10', 110.00, 'FedEx', 4215),
('T-041', 'E-137', 'Sales', '2025-02-11', 190.00, 'Southwest', 4511),
('T-042', 'E-138', 'IT', '2025-02-12', 300.00, 'GitHub', 7372),
('T-043', 'E-139', 'HR', '2025-02-13', 60.00, 'Target', 5310),
('T-044', 'E-140', 'Sales', '2025-02-14', 250.00, 'Flowers 4 U', 5992),
('T-045', 'E-141', 'Marketing', '2025-02-15', 500.00, 'Twitter', 7311), -- Weekend
('T-046', 'E-142', 'Finance', '2025-02-16', 75.00, 'Movie Theater', 7832), -- Restricted + Weekend
('T-047', 'E-143', 'Sales', '2025-02-17', 140.00, 'Budget Rent', 3351),
('T-048', 'E-144', 'IT', '2025-02-18', 1200.00, 'Oracle', 7372),
('T-049', 'E-145', 'HR', '2025-02-19', 40.00, 'Panera', 5814),
('T-050', 'E-146', 'Sales', '2025-02-20', 300.00, 'Hilton', 3501);

-- 1.2 Populate MCC_Codes
INSERT INTO ExpenseAuditDB.Bronze.MCC_Codes (Code, Category, IsRestricted) VALUES
(3501, 'Hotels', false),
(4511, 'Airlines', false),
(5812, 'Restaurants', false),
(5814, 'Fast Food', false),
(4121, 'Taxis/Rideshare', false),
(5732, 'Electronics', false),
(5943, 'Office Supplies', false),
(7311, 'Advertising', false),
(7372, 'Computer Software', false),
(7995, 'Betting/Casino', true),
(5921, 'Package Stores/Beer/Wine', true),
(7992, 'Golf Courses', true),
(7298, 'Health and Beauty Spas', true),
(7832, 'Motion Picture Theaters', true);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Policy Checks
-------------------------------------------------------------------------------
-- Description: Joining Txns with MCC to flag restricted codes and weekend dates.
-- Feature: Flag 'IsViolation' = true if MCC is restricted OR DayOfWeek = Sat/Sun.

CREATE OR REPLACE VIEW ExpenseAuditDB.Silver.FlaggedExpenses AS
SELECT
    t.TxnID,
    t.EmployeeID,
    t.Department,
    t.TxnDate,
    t.Amount,
    t.MerchantName,
    m.Category,
    m.IsRestricted,
    EXTRACT(DAYOFWEEK FROM t.TxnDate) AS DayNum, -- 1=Sun, 7=Sat
    CASE 
        WHEN m.IsRestricted = true THEN 'Restricted Category'
        WHEN EXTRACT(DAYOFWEEK FROM t.TxnDate) IN (1, 7) THEN 'Weekend Spend'
        ELSE 'OK'
    END AS ViolationReason
FROM ExpenseAuditDB.Bronze.ExpenseTxns t
LEFT JOIN ExpenseAuditDB.Bronze.MCC_Codes m ON t.MCC_Code = m.Code;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Audit Report
-------------------------------------------------------------------------------
-- Description: Aggregating violations by Department monitoring.
-- Insight: identifying departments with culture issues.

CREATE OR REPLACE VIEW ExpenseAuditDB.Gold.AbuseByDepartment AS
SELECT
    Department,
    COUNT(TxnID) AS TotalTxns,
    SUM(CASE WHEN ViolationReason <> 'OK' THEN 1 ELSE 0 END) AS ViolationCount,
    SUM(CASE WHEN ViolationReason <> 'OK' THEN Amount ELSE 0 END) AS ViolationAmount
FROM ExpenseAuditDB.Silver.FlaggedExpenses
GROUP BY Department;

CREATE OR REPLACE VIEW ExpenseAuditDB.Gold.ViolationDetails AS
SELECT * 
FROM ExpenseAuditDB.Silver.FlaggedExpenses
WHERE ViolationReason <> 'OK'
ORDER BY TxnDate DESC;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show the total ViolationAmount per Department from ExpenseAuditDB.Gold.AbuseByDepartment, sorted descending."

PROMPT:
"List all transactions from ExpenseAuditDB.Gold.ViolationDetails where the ViolationReason is 'Restricted Category'."
*/
