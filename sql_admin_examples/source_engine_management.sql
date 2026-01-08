-------------------------------------------------------------------------------
-- Dremio SQL Admin Examples: Source & Engine Management
-- 
-- This script covers Source configuration, Metadata refreshes, and Engine routing.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SOURCE MANAGEMENT
-------------------------------------------------------------------------------

-- 1.1 Refresh Metadata
-- Forces Dremio to re-read the schema/file list from the source (e.g., S3).
-- Use FORCE UPDATE for full refresh.
ALTER SOURCE "AWS_DataLake" REFRESH STATUS;

-- 1.2 Refresh Specific Table within Source
ALTER TABLE "AWS_DataLake"."Raw_Logs" REFRESH METADATA;

-- 1.3 Promote File System Folder to Table (PDS)
-- Formats a folder as a Physical Dataset.
ALTER TABLE "AWS_DataLake"."Incoming_Data"."Clickstream"
REFRESH METADATA
AUTO PROMOTION;

-------------------------------------------------------------------------------
-- 2. ENGINE MANAGEMENT (Workload Routing)
-- Note: Applies primarily to Dremio Cloud or Enterprise scenarios.
-------------------------------------------------------------------------------

-- 2.1 Route Session to Specific Engine
-- Useful for isolating ETL workloads from Dashboard/Interactive workloads.
SET ENGINE = 'ETL_HighMem_Engine';

-- 2.2 Reset Engine Routing (Default)
SET ENGINE = DEFAULT;

-------------------------------------------------------------------------------
-- 3. SESSION & CONTEXT
-------------------------------------------------------------------------------

-- 3.1 Set Query Context
-- avoids typing full paths (e.g., SELECT * FROM "Sales" vs "DB"."Schema"."Sales").
USE "MarketingDB"."Gold";

-- 3.2 System Session Settings
-- Set specific planner options for the current session.
ALTER SESSION SET "planner.memory.limit" = 2000000000; -- Example bytes
ALTER SESSION SET "store.parquet.compression" = 'snappy';

-------------------------------------------------------------------------------
-- 4. TAGGING (Governance)
-- Tagging datasets for discovery and governance.
-------------------------------------------------------------------------------

-- 4.1 Set Tags on a Wiki/Dataset
-- Note: Often done via UI/API, but SQL support varies.
-- ALTER TABLE "MarketingDB"."Gold"."Campaigns" SET TAGS ('PII', 'GDPR');
