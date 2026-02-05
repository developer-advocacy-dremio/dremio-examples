/*
 * Dremio "Messy Data" Challenge: Broken Healthcare Claims
 * 
 * Scenario: 
 * Claims data scanned via OCR from different regional providers.
 * 'Service_Date' has mixed formats: 'MM-DD-YYYY', 'DD/MM/YYYY', 'YYYY-MM-DD'.
 * Some amounts have '$' symbols or ',' separators that need cleaned.
 * 
 * Objective for AI Agent:
 * 1. Standardize 'Service_Date' to a valid DATE format.
 * 2. Clean 'Billed_Amount' to a numeric Double.
 * 3. Reconcile Provider IDs using the Master list.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Claims_Clearinghouse;
CREATE FOLDER IF NOT EXISTS Claims_Clearinghouse.OCR_In;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Claims_Clearinghouse.OCR_In.CLAIM_DUMP (
    CL_ID VARCHAR,
    P_ID VARCHAR, -- Patient
    OC_DT VARCHAR, -- 'Service Date' raw string
    BILL_STR VARCHAR, -- '$1,000.00'
    PROV_REF VARCHAR
);

CREATE TABLE IF NOT EXISTS Claims_Clearinghouse.OCR_In.PROV_MASTER (
    PROV_ID VARCHAR,
    NAME VARCHAR,
    REGION VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Mixed Formats)
-------------------------------------------------------------------------------

-- Provider Master
INSERT INTO Claims_Clearinghouse.OCR_In.PROV_MASTER VALUES
('PR-01', 'General Hospital', 'East'),
('PR-02', 'City Clinic', 'West'),
('PR-03', 'Dr. Smith', 'North');

-- Claims
-- US Format (MM-DD-YYYY)
INSERT INTO Claims_Clearinghouse.OCR_In.CLAIM_DUMP VALUES
('C-001', 'P-100', '01-15-2023', '$500.00', 'PR-01'),
('C-002', 'P-100', '01-16-2023', '$250.50', 'PR-01');

-- UK/EU Format (DD/MM/YYYY) - Ambiguous!
INSERT INTO Claims_Clearinghouse.OCR_In.CLAIM_DUMP VALUES
('C-003', 'P-101', '13/01/2023', '1.200,00', 'PR-02'), -- 13th Jan
('C-004', 'P-101', '01/02/2023', '100.00', 'PR-02'); -- Feb 1st (EU)

-- ISO Format
INSERT INTO Claims_Clearinghouse.OCR_In.CLAIM_DUMP VALUES
('C-005', 'P-102', '2023-03-10', '400', 'PR-03');

-- Bad Amounts / Bad Dates
INSERT INTO Claims_Clearinghouse.OCR_In.CLAIM_DUMP VALUES
('C-006', 'P-103', 'Unknown', 'Free', 'PR-01'),
('C-007', 'P-103', '2023-01-01', '$1,000.00-', 'PR-01'); -- Trailing negative sign

-- Bulk Fill
INSERT INTO Claims_Clearinghouse.OCR_In.CLAIM_DUMP VALUES
('C-010', 'P-104', '12-31-2022', '$100', 'PR-01'),
('C-011', 'P-104', '01-01-2023', '$150', 'PR-01'),
('C-012', 'P-104', '01-02-2023', '$200', 'PR-01'),
('C-013', 'P-104', '01-03-2023', '$250', 'PR-01'),
('C-014', 'P-104', '01-04-2023', '$300', 'PR-01'),
('C-015', 'P-105', '05/01/2023', '300,50', 'PR-02'), -- May 1st or Jan 5th? assume consistent per provider
('C-016', 'P-105', '06/01/2023', '300,50', 'PR-02'),
('C-017', 'P-105', '07/01/2023', '300,50', 'PR-02'),
('C-018', 'P-105', '08/01/2023', '300,50', 'PR-02'),
('C-019', 'P-105', '09/01/2023', '300,50', 'PR-02'),
('C-020', 'P-106', '2023-04-01', '500', 'PR-03'),
('C-021', 'P-106', '2023-04-02', '550', 'PR-03'),
('C-022', 'P-106', '2023-04-03', '600', 'PR-03'),
('C-023', 'P-106', '2023-04-04', '650', 'PR-03'),
('C-024', 'P-106', '2023-04-05', '700', 'PR-03'),
('C-025', 'P-107', 'Oct 1, 2023', '$1,000', 'PR-01'), -- Textual date
('C-026', 'P-107', 'Oct 2, 2023', '$1,000', 'PR-01'),
('C-027', 'P-107', 'Oct 3, 2023', '$1,000', 'PR-01'),
('C-028', 'P-107', 'Oct 4, 2023', '$1,000', 'PR-01'),
('C-029', 'P-107', 'Oct 5, 2023', '$1,000', 'PR-01'),
('C-030', 'P-108', '2023.11.01', '$50', 'PR-03'), -- Dots
('C-031', 'P-108', '2023.11.02', '$50', 'PR-03'),
('C-032', 'P-108', '2023.11.03', '$50', 'PR-03'),
('C-033', 'P-108', '2023.11.04', '$50', 'PR-03'),
('C-034', 'P-108', '2023.11.05', '$50', 'PR-03'),
('C-035', 'P-109', '01-01-23', '$500', 'PR-01'), -- 2-digit year
('C-036', 'P-109', '01-02-23', '$500', 'PR-01'),
('C-037', 'P-109', '01-03-23', '$500', 'PR-01'),
('C-038', 'P-109', '01-04-23', '$500', 'PR-01'),
('C-039', 'P-109', '01-05-23', '$500', 'PR-01'),
('C-040', 'P-110', 'NULL', 'NULL', 'PR-01'),
('C-041', 'P-110', 'NULL', 'NULL', 'PR-01'),
('C-042', 'P-110', 'NULL', 'NULL', 'PR-01'),
('C-043', 'P-110', 'NULL', 'NULL', 'PR-01'),
('C-044', 'P-110', 'NULL', 'NULL', 'PR-01'),
('C-045', 'P-111', '2023-12-25', '1 000.00', 'PR-02'), -- Space separator
('C-046', 'P-111', '2023-12-26', '1 000.00', 'PR-02'),
('C-047', 'P-111', '2023-12-27', '1 000.00', 'PR-02'),
('C-048', 'P-111', '2023-12-28', '1 000.00', 'PR-02'),
('C-049', 'P-111', '2023-12-29', '1 000.00', 'PR-02'),
('C-050', 'P-112', '2023-12-30', '$1K', 'PR-03'); -- K notation

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "I have OCR claim data in Claims_Clearinghouse.OCR_In.CLAIM_DUMP.
 *  The dates and amounts are messy strings. Clean this data:
 *  
 *  1. Bronze: View for raw data.
 *  2. Silver: 
 *     - coerce 'OC_DT' (Service Date) into a valid ISO DATE type, handling MM-DD vs DD/MM formats.
 *     - Clean 'BILL_STR' (remove '$', ',', spaces) and cast to DOUBLE.
 *  3. Gold: 
 *     - Sum the cleaned Billed Amount by Provider Name (Join with PROV_MASTER).
 *  
 *  Generate the SQL."
 */
