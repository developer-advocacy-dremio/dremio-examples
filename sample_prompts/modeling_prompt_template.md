# Dremio Medallion Architecture Prompt Template

Use this prompt with an LLM (like ChatGPT, Claude, or Gemini) to generate a complete Bronze-Silver-Gold SQL script for Dremio.

## Instructions
1. Copy the prompt below.
2. Fill in the placeholder values (in `[BRACKETS]`) with your specific details.
3. Paste into your LLM of choice.

---

## The Prompt

**Role**: You are a Data Engineer expert in Dremio and the Medallion Architecture (Bronze -> Silver -> Gold).

**Goal**: Create a Dremio SQL script that builds a Medallion architecture for the following datasets and metrics.

**Context**:
I have the following existing datasets in Dremio (Raw/Source):
1. `[DATASET_1_NAME]` (Description: `[DESCRIPTION_1]`)
2. `[DATASET_2_NAME]` (Description: `[DESCRIPTION_2]`)
3. `[DATASET_3_NAME]` (Description: `[DESCRIPTION_3]`)
*(Add more as needed)*

**Objective**:
I need to calculate the following metrics:
- `[METRIC_1]`
- `[METRIC_2]`
- `[METRIC_3]`
*(Add more as needed)*

**Requirements**:
Generate a single SQL script that performs the following steps:

1.  **Folder Creation**: Create a folder named `[FOLDER_NAME]` to house the views.
2.  **Bronze Layer**: Create views in `[FOLDER_NAME]` for the raw datasets. These should be direct 1:1 views of the source, perhaps with simple renaming or casting if necessary. Name them with a `_bronze` suffix.
3.  **Silver Layer**: Create views in `[FOLDER_NAME]` that join, clean, and deduplicate the Bronze views. This layer should handle data quality issues and prepare the data for aggregation. Name them with a `_silver` suffix.
4.  **Gold Layer**: Create views in `[FOLDER_NAME]` that aggregate the Silver data to calculate the requested metrics. These should be business-ready datasets. Name them with a `_gold` suffix.

**Output Format**:
- Provide valid Dremio SQL code.
- Use `CREATE OR REPLACE VIEW` syntax.
- Add comments explaining each step.
- Ensure the logic follows best practices for Dremio (e.g., avoiding expensive operations where possible).

**Specific Inputs**:
- **Target Folder**: `[TARGET_FOLDER_PATH]` (e.g., "MySpace.MyFolder")
- **Source Data Paths**:
    - `[DATASET_1_PATH]`
    - `[DATASET_2_PATH]`
    - ...
