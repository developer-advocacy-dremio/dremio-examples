# Dremio Agent Skill (External Tools) 🤖

This guide explains how to use the **Dremio Agent Skill** with external AI tools like **Claude Code**, **Cursor**, **Antigravity**, or **VS Code Copilot**.

## 🚀 What is the Dremio Agent Skill?

The [Dremio Agent Skill](https://github.com/developer-advocacy-dremio/dremio-agent-skill) is a "Knowledge Package" that you install into your AI assistant. It parses Dremio's documentation for you, teaching the AI exactly how to use:

1.  **`dremioframe`**: A specialized Python DataFrame library for Dremio.
2.  **`dremio-cli`**: A command-line tool for Dremio administration.
3.  **Advanced SQL**: Iceberg DML, Metadata functions, and optimized syntax.

Once installed (by determining the `dremio-skill` folder in your project), you don't need to explain *how* to connect—you just ask *what* you want.

---

## 🗣️ Semantic Prompts (Requires Skill)

Because the agent now "knows" Dremio's tools, your prompts can be high-level and semantic.

### 🐍 Python Automation (`dremioframe`)

Instead of asking for raw `requests` code, ask the agent to use `dremioframe` for cleaner, more robust scripts.

**1. Data Movement & Admin**
> "Write a Python script using `dremioframe` to clone the space 'Marketing' into a new space called 'Marketing_Archive'."

**2. Lineage Analysis**
> "Use `dremioframe` to find all datasets that are downstream of the table 'Sales.Transactions'. Print them as a tree."

**3. User Management**
> "Generate a script using `dremioframe` to create 50 users from a local CSV file `users.csv`."

**4. Data Extraction**
> "Write a script to fetch the top 1000 rows from 'Finance.Q1_Report' and load them into a local Polars dataframe using `dremioframe`."

---

### 💻 CLI Operations (`dremio-cli`)

The agent has learned the full CLI reference.

**1. Environment Promotion**
> "Give me the `dremio-cli` command to export the entire 'Finance' space to a generic JSON file for backup."

**2. Reflection Management**
> "Write a bash script using `dremio-cli` that iterates through all Physical Datasets in 'Datalake' and triggers a reflection refresh."

**3. Catalog Operations**
> "How do I use the CLI to promote a folder on S3 to a Physical Dataset?"

---

### 📊 Advanced SQL & Iceberg

The agent knows Dremio's specific SQL dialect, including Iceberg management.

**1. Iceberg Optimization**
> "Write a Dremio SQL query to OPTIMIZE the 'Sales.Events' table and VACUUM snapshots older than 7 days."

**2. Metadata Exploration**
> "How do I query the Iceberg metadata to see the snapshot history of table 'Logs'?"

**3. Complex Query Rewrite**
> "Rewrite this query to use Dremio's specific PIVOT syntax instead of CASE WHEN statements."

---

## 📥 How to Install the Skill

1.  **Download**: Clone or download the [repo](https://github.com/developer-advocacy-dremio/dremio-agent-skill).
2.  **Copy**: Place the `dremio-skill/` folder in your project root.
3.  **Configure**:
    *   **Cursor/VS Code**: Copy content from `dremio-skill/rules/.cursorrules` to your `.cursorrules`.
    *   **Claude/Antigravity**: Ensure the `dremio-skill/SUB_SKILL.md` is discoverable by the agent.
