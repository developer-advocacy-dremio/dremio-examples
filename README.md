# Dremio Lakehouse Examples & Usage Guide

**Welcome to the ultimate Dremio Lakehouse resource.**

This repository contains over **240+ industry-specific SQL scenarios**, tailored to demonstrate the power of Dremio's Medallion Architecture (Bronze -> Silver -> Gold). It also serves as a comprehensive reference for Dremio's integrations with the broader data ecosystem.

## 🗂️ Example Libraries

### 1. [Standard Industry Examples (200+)](sql_examples/README.md)
Contains standard vertical use cases including:
-   **Healthcare** (Clinical, Claims, Operations)
-   **Finance** (Banking, Trading, Risk)
-   **Government** (Public Safety, Infrastructure)
-   **Retail & Supply Chain** (Logistics, Customer 360)

### 2. [Emerging Tech & Specialized (40+)](sql_examples2/README.md)
Contains new (2025-2026) scenarios focusing on:
-   **IoT & Sustainability** (Hydroponics, EV Battery, Green Hydrogen)
-   **Advanced Tech** (Orbital Debris, Drone Airspace, CRISPR)
-   **Modern Economy** (Gig Economy, Esports, Influencer ROI)

---

## 🛠️ Technical Resources

### SQL Reference
-   **[SQL Functions](sql_function_examples/README.md)**: Math, String, Date, Window, and Aggregates.
-   **[AI Functions](sql_ai_function_examples/README.md)**: Sentiment analysis and data transformation with LLMs.
-   **[Admin & Maintenance](sql_admin_examples/README.md)**: RBAC, Reflections, Vacuum, and Optimization.
-   **[System Tables](sql_system_table_examples/README.md)**: Job analytics and audit logs.
-   **[Ingestion & DML](sql_ingestion_examples/README.md)**: `COPY INTO`, `MERGE`, and Auto-ingest.
-   **[Iceberg Metadata](sql_iceberg_metadata_examples/README.md)**: Querying snapshots, manifests, and partitions.

### Integrations & Connectivity
-   **[Dremio Interfaces](dremio_interfaces/README.md)**: Python (Flight, ODBC/JDBC, REST API).
-   **[Other Engines](other_engines/README.md)**: Connecting Spark, Flink, Trino, and others to Dremio's Catalog.

### GenAI & Agents
-   **[Sample Prompts](sample_prompts/README.md)**: Templates for SQL generation and Agent skills.

### Additional Scenarios
-   **[Customer 360](customer360/README.md)**: Unified customer views.
-   **[Hybrid Supply Chain](hybrid_supply_chain/README.md)**: Federation across clouds.
-   **[More Examples](more_examples/README.md)**: Autonomous vehicle fleets and other misc scenarios.

---

## 🏛️ Architecture & Best Practices

All examples follow the **Medallion Architecture**:
-   **🥉 Bronze**: Raw ingestion tables.
-   **🥈 Silver**: Cleaned, verified views.
-   **🥇 Gold**: Business-level aggregates.

**Key Guides:**
-   **[View & Reflection Strategy](view_reflection_best_practices.md)**: How to layer views and accelerate queries.
-   **[Masking & Security](masking_recipes.md)**: Implementing Row-Level Security (RLS) and Column Masking.
-   **[Privilege Management](privilege_management_recipes.md)**: Managing access control via RBAC.
