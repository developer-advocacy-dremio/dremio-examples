# Dremio Lakehouse Examples & Usage Guide

**Welcome to the ultimate Dremio Lakehouse resource.**

This repository contains over **240+ industry-specific SQL scenarios**, tailored to demonstrate the power of Dremio's Medallion Architecture (Bronze -> Silver -> Gold). It also serves as a comprehensive reference for Dremio's integrations with the broader data ecosystem.

## 🗂️ Example Libraries

### 1. [Standard Industry Examples](sql_examples/README.md)
Contains standard vertical use cases including:
-   **Healthcare** (Clinical, Claims, Operations)
-   **Finance** (Banking, Trading, Risk)
-   **Government** (Public Safety, Infrastructure)
-   **Retail & Supply Chain** (Logistics, Customer 360)

### 2. [Emerging Tech & Specialized](sql_examples2/README.md)
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

## 🔬 Niche, Scientific & Non-Profit Domains (Phase 3)

| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Wildlife Conservation** | Tracking animal tagging data to identify poaching risks. | `Geospatial`, `Medallion` | [wildlife_conservation_tracking.sql](sql_examples/wildlife_conservation_tracking.sql) |
| **Ocean Plastic Cleanup** | Optimizing vessel routes based on satellite density maps. | `RouteOptimization` | [ocean_plastic_cleanup.sql](sql_examples/ocean_plastic_cleanup.sql) |
| **Digital Archaeology** | Automating artifact dating via stratigraphic analysis. | `Stratigraphy` | [digital_archaeology_site.sql](sql_examples/digital_archaeology_site.sql) |
| **Ski Resort Ops** | Balancing lift wait times with snow grooming quality. | `UtilizationMetric` | [ski_resort_operations.sql](sql_examples/ski_resort_operations.sql) |
| **Refugee Aid Dist.** | Forecasting food/water gaps for camp demographics. | `GapAnalysis` | [humanitarian_aid_distribution.sql](sql_examples/humanitarian_aid_distribution.sql) |
| **Nuclear Waste Monitor** | Tracking thermal decay and pressure for safety alerts. | `ThresholdDetection` | [nuclear_waste_monitoring.sql](sql_examples/nuclear_waste_monitoring.sql) |
| **Urban Noise Pollution** | Mapping sensor decibels to zoning for compliance. | `GeospatialCompliance` | [urban_noise_pollution.sql](sql_examples/urban_noise_pollution.sql) |
| **Patent Portfolio Mgmt** | Valuing IP assets via forward citation graph analysis. | `GraphAnalysis` | [legal_patent_portfolio.sql](sql_examples/legal_patent_portfolio.sql) |
| **Radio Astronomy** | Filtering RFI interference to detect pulsars/FRBs. | `SignalProcessing` | [astronomy_radio_telescope.sql](sql_examples/astronomy_radio_telescope.sql) |
| **Luxury Counterfeit** | Detecting cloned tags via impossible travel time logic. | `AntiCounterfeit` | [luxury_counterfeit_detection.sql](sql_examples/luxury_counterfeit_detection.sql) |
