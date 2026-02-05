/*
 * Dremio "Messy Data" Challenge: Duplicate Loyalty Accounts
 * 
 * Scenario: 
 * Retail loyalty program has duplicate signups.
 * 'Phone' and 'Email' are used as identifiers but people switch them.
 * Some accounts have points spread across duplicates.
 * 
 * Objective for AI Agent:
 * 1. Identify distinct 'Persons' based on matching Email OR Phone.
 * 2. Group duplicates together.
 * 3. Sum 'Points_Balance' for the grouped entity.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Loyalty_Program;
CREATE FOLDER IF NOT EXISTS Loyalty_Program.Members;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Loyalty_Program.Members.ACCOUNTS (
    ACCT_ID VARCHAR,
    NAME VARCHAR,
    EMAIL VARCHAR,
    PHONE VARCHAR,
    POINTS INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Entity Resolution)
-------------------------------------------------------------------------------

-- Person A (Matches by Email)
INSERT INTO Loyalty_Program.Members.ACCOUNTS VALUES
('A-001', 'John Doe', 'john@email.com', '555-0001', 100),
('A-002', 'J. Doe', 'john@email.com', '555-9999', 50); -- Different phone, same email

-- Person B (Matches by Phone)
INSERT INTO Loyalty_Program.Members.ACCOUNTS VALUES
('A-003', 'Jane Smith', 'jane@work.com', '555-1234', 200),
('A-004', 'Jane S', 'jane@home.com', '555-1234', 100); -- Different email, same phone

-- Person C (Transitive Match? A=B, B=C -> A=C) -> Hard in SQL!
-- Let's stick to direct Single-Hop matches for now.
INSERT INTO Loyalty_Program.Members.ACCOUNTS VALUES
('A-005', 'Bob', 'bob@mail.com', '555-5555', 10),
('A-006', 'Bobby', 'bob@mail.com', NULL, 20),
('A-007', 'Robert', NULL, '555-5555', 30); -- Matches 005 by phone, 005 matches 006 by email

-- Bulk Fill
INSERT INTO Loyalty_Program.Members.ACCOUNTS VALUES
('A-008', 'Alice', 'alice@mail.com', '111-1111', 100),
('A-009', 'Alice', 'alice@mail.com', '111-1111', 100), -- Exact duplicate
('A-010', 'Carol', 'carol@mail.com', '222-2222', 500),
('A-011', 'Dave', 'dave@mail.com', '333-3333', 100),
('A-012', 'Davey', 'dave@mail.com', '333-0000', 50),
('A-013', 'David', 'david@other.com', '333-3333', 50),
('A-014', 'Eve', 'eve@mail.com', '444-4444', 10),
('A-015', 'Eve', 'eve@corp.com', '444-5555', 20), -- No match
('A-016', 'Frank', 'frank@mail.com', '666-6666', 1000),
('A-017', 'Grace', 'grace@mail.com', '777-7777', 100),
('A-018', 'Heidi', 'heidi@mail.com', '888-8888', 100),
('A-019', 'Ivan', 'ivan@mail.com', '999-9999', 100),
('A-020', 'Judy', 'judy@mail.com', '000-0000', 100),
('A-021', 'Kevin', 'kevin@mail.com', '123-4567', 50),
('A-022', 'Kev', 'kevin@mail.com', '123-4567', 50),
('A-023', 'Larry', 'larry@mail.com', '111-2222', 50),
('A-024', 'Mike', 'mike@mail.com', '222-3333', 50),
('A-025', 'Norah', 'norah@mail.com', '333-4444', 50),
('A-026', 'Ophelia', 'ophelia@mail.com', '444-5555', 50),
('A-027', 'Paul', 'paul@mail.com', '555-6666', 50),
('A-028', 'Quinn', 'quinn@mail.com', '666-7777', 50),
('A-029', 'Randy', 'randy@mail.com', '777-8888', 50),
('A-030', 'Steve', 'steve@mail.com', '888-9999', 50),
('A-031', 'Tom', 'tom@mail.com', '999-0000', 50),
('A-032', 'Ursula', 'ursula@mail.com', '000-1111', 50),
('A-033', 'Victor', 'victor@mail.com', '111-1212', 50),
('A-034', 'Wendy', 'wendy@mail.com', '222-2323', 50),
('A-035', 'Xavier', 'xavier@mail.com', '333-3434', 50),
('A-036', 'Yvonne', 'yvonne@mail.com', '444-4545', 50),
('A-037', 'Zach', 'zach@mail.com', '555-5656', 50),
('A-038', 'Zachary', 'zach@mail.com', '555-5656', 20),
('A-039', 'Zack', 'zack@mail.com', '555-5656', 10), -- Mismatch email, match phone?
('A-040', 'Z.', 'z@mail.com', '999-9999', 100), -- Duplicate Ivan phone
('A-041', 'NULL', NULL, NULL, 0),
('A-042', '', '', '', 0),
('A-043', 'NoPoints', 'np@mail.com', '000-0000', 0),
('A-044', 'NegPoints', 'neg@mail.com', '000-0000', -50),
('A-045', 'Max', 'max@mail.com', '123-1234', 1000000),
('A-046', 'Min', 'min@mail.com', '123-1234', 1),
('A-047', 'Avg', 'avg@mail.com', '123-1234', 500),
('A-048', 'Sam', 'sam@mail.com', '555-1111', 10),
('A-049', 'S.', 'sam@mail.com', '555-1111', 10),
('A-050', 'Samuel', 'sam@mail.com', '555-1111', 10);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Deduplicate loyalty accounts in Loyalty_Program.Members.ACCOUNTS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Coalesce: Create a 'Master_Key' using Email, or Phone if Email is null.
 *     - Identify 'Unified_User': Group by Email (if present) OR Phone.
 *  3. Gold: 
 *     - Sum 'Points' by Unified_User.
 *     - List all variants of Names associated with that user.
 *  
 *  Provide the SQL."
 */
