# Dremio Built-in Agent Library 🤖

This library contains prompts optimized for **Dremio's Integrated AI Agent** (Text-to-SQL). 
These prompts are designed to generate **SQL** code that can be immediately executed in the Dremio SQL Runner.

> **Note**: The built-in agent **cannot** execute Python or call external APIs. For those tasks, see the [External Agent Skills](external_agent_skills.md) guide.

---

## 📚 Documentation & Semantics

### 1. Semantic Layer Documenter
**Goal**: Generate a Wiki description for the dataset you are currently viewing.
**Action**: Open the dataset in Dremio, then ask the agent:
```text
Act as a Data Steward.
Generate a comprehensive "Wiki" entry for this dataset based on its schema.

Include:
1.  **Executive Summary**: 1-sentence description.
2.  **Business Value**: Why business users need this data.
3.  **Column Glossary**: A table describing business terms for each column.
4.  **Sample Questions**: 3 questions this dataset answers.
```

### 2. Business Question Solver
**Goal**: Convert a natural language question into a run-ready SQL query.
**Action**: In the Text-to-SQL chat bar:
```text
Answer the following business question using the tables in the current space.

Question: "[INSERT YOUR QUESTION HERE]"

Requirements:
- Use Dremio SQL syntax.
- Auto-detect table names from context.
- explain the logic before generating the SQL.
```

---

## ⚡ Performance & Tuning

### 3. Reflections Advisor
**Goal**: Get recommendations for Reflection definitions to speed up queries.
**Action**: While viewing a dataset (Physical or Virtual):
```text
Act as a Dremio Performance Engineer. 
Analyze this dataset's schema and statistics. 
Recommend a "Reflection" strategy (Raw or Aggregation) to accelerate common dashboarding patterns.

Constraints:
- Identify high-cardinality columns for Dimensions.
- Identify numeric columns for Measures.
- Focus on query acceleration.
```

### 4. Query Optimizer
**Goal**: Improve the performance of a specific SQL query.
**Action**: Paste the SQL into the chat or reference a Job ID.
```text
Act as a Database Administrator. Optimize this SQL query:

[PASTE SQL]

Look for:
1.  Cartesian products (Cross Joins).
2.  Inefficient casting (e.g. string to date).
3.  Calculations in WHERE clauses (non-sargable).

Provide the rewritten SQL.
```

---

## 🛠️ Data Engineering

### 5. Complex JSON Flattener
**Goal**: Flatten nested JSON structures into a tabular view.
**Action**: Ask the agent to transform the current dataset.
```text
I need to flatten the nested column '[COLUMN_NAME]'.
Write a SQL query using the `FLATTEN` command or dot-notation to extract all leaf fields into top-level columns.
Preserve the other columns but replace the nested column with its flattened fields.
```

### 6. Iceberg Maintenance Scripts
**Goal**: Generate maintenance SQL for Iceberg tables.
**Action**: Navigate to an Iceberg table and ask:
```text
Act as a Data Ops Engineer. 
Generate a maintenance script for this Iceberg table.

Include:
1.  `OPTIMIZE TABLE` command to compact small files.
2.  `VACUUM TABLE` command to expire snapshots older than 7 days.
```

### 7. Date Dimension Generator
**Goal**: Create a standard calendar table using SQL.
**Action**: Ask the agent to generate the DDL/DML.
```text
Act as a SQL expert. Write a generic Dremio SQL script to generate a "Date Dimension" table.
It should generate one row per day from 2023-01-01 to 2025-12-31.

Columns needed:
- DateKey (YYYYMMDD)
- FullDate (DATE)
- DayOfWeek (String)
- MonthName
- Quarter (Q1-Q4)
- IsWeekend (Boolean)
```

---

## � Security

### 8. RBAC Policy Generator
**Goal**: Create a Row-Level Security policy (UDF).
**Action**: Ask the agent to write the security logic.
```text
Act as a Security Admin. 
I need a Row-Level Security policy for this table filtering by the '[COUNTRY_COLUMN]' column.

Rules:
- 'Global_Admins' see ALL data.
- 'US_Managers' see 'USA'.
- 'EU_Managers' see 'UK' or 'France'.

Output:
Write the SQL to create a scalar UDF that returns BOOLEAN to use as the policy.
```
