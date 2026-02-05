# Dremio SQL Examples Phase 8: Raw & Messy Data
## "The AI Agent's Crucible"

This directory (`sql_examples3`) contains 10 SQL scenarios designed specifically to **challenge and train Dremio AI Agents**.

Unlike the previous examples, these datasets are **intentionally "dirty"**. They simulate real-world data dumps from legacy systems, OCR scans, and loosely governed IoT networks.

### The Objective
An AI Agent analyzing these files should not just "run them". It should be able to:
1.  **Detect Issues**: Identify duplicates, nulls, outliers, and schema drift.
2.  **Propose cleaning strategies**: Regex patterns, fuzzy matching, deduplication logic.
3.  **Architect Solutions**: Design a Medallion Architecture (Bronze -> Silver -> Gold) to sanitize this data.

### The Scenarios

| File | Domain | "Messy" Characteristics |
| :--- | :--- | :--- |
| **[legacy_banking_system.sql](legacy_banking_system.sql)** | Banking | Opaque column names (`C1`, `C2`), duplicate keys, deleted flags. |
| **[messy_iot_sensors.sql](messy_iot_sensors.sql)** | IoT | Timestamp logic errors, calibration outliers (`-9999`), exact duplicates. |
| **[obfuscated_ad_clicks.sql](obfuscated_ad_clicks.sql)** | AdTech | Key mismatches (`INT` vs `HASH`), timezone alignment issues. |
| **[broken_healthcare_claims.sql](broken_healthcare_claims.sql)** | Healthcare | Mixed date formats (`MM-DD` vs `DD-MM`), OCR currency artifacts. |
| **[unstructured_retail_inventory.sql](unstructured_retail_inventory.sql)** | Retail | Fuzzy string matching needed for product names, mixed currency symbols. |
| **[fragmented_logistics_tracking.sql](fragmented_logistics_tracking.sql)** | Logistics | Split data across tables, overlapping IDs, inconsistent status codes. |
| **[erroneous_energy_readings.sql](erroneous_energy_readings.sql)** | Energy | Impossible physics (negative amps), massive outlier filtering. |
| **[dirty_telecom_cdr.sql](dirty_telecom_cdr.sql)** | Telecom | Regex cleaning for phone numbers (`+1`, `.`, `-`), duration unit normalization. |
| **[mismatched_hr_records.sql](mismatched_hr_records.sql)** | HR | Entity resolution across tables using fuzzy logic on Names/Emails. |
| **[json_heavy_game_events.sql](json_heavy_game_events.sql)** | Gaming | Extraction of fields from varying JSON schemas stored in a string column. |
| **[messy_airline_logs.sql](messy_airline_logs.sql)** | Airline | IATA vs ICAO codes, timezone offsets leading to negative flight times. |
| **[dirty_ecommerce_clickstream.sql](dirty_ecommerce_clickstream.sql)** | E-commerce | Bot traffic filtering (User-Agents), URL parameter parsing. |
| **[fragmented_supply_chain.sql](fragmented_supply_chain.sql)** | Supply Chain | Unit mismatches (Kg vs Lbs), currency conversion (EUR vs USD). |
| **[unstructured_real_estate.sql](unstructured_real_estate.sql)** | Real Estate | Address parsing (Street/City/Zip), unstructured amenities JSON. |
| **[noisy_social_media.sql](noisy_social_media.sql)** | Social Media | Emoji/Non-ASCII spam detection, relative timestamp normalized. |
| **[mixed_weather_data.sql](mixed_weather_data.sql)** | Weather | Fahrenheit vs Celsius units, broken sensor codes (9999). |
| **[drifting_ride_share.sql](drifting_ride_share.sql)** | Ride Share | GPS drift/outliers (Null Island), duplicate status events. |
| **[legacy_hospital_records.sql](legacy_hospital_records.sql)** | Hospital | Gender encoding drift (M/F vs 1/2), DOB standardization. |
| **[chaotic_university_enrollment.sql](chaotic_university_enrollment.sql)** | Education | Student ID migration (Old vs New format), Grade mixing (Letter vs GPA). |
| **[duplicate_loyalty_accounts.sql](duplicate_loyalty_accounts.sql)** | Retail | Entity resolution for users with multiple email/phone signups. |
| **[streaming_service_buffering.sql](streaming_service_buffering.sql)** | Streaming | Session concurrency (device sharing), buffering event chaining. |
| **[legacy_library_catalog.sql](legacy_library_catalog.sql)** | Library | ISBN-10 vs 13, Roman numeral dates, Call number sorting. |
| **[hotel_booking_overlaps.sql](hotel_booking_overlaps.sql)** | Hotel | Double bookings (date overlap), room type code confusion. |
| **[car_maintenance_history.sql](car_maintenance_history.sql)** | Automotive | Odometer rollbacks (decreasing values), VIN typos (OCR errors). |
| **[insurance_policy_renewals.sql](insurance_policy_renewals.sql)** | Insurance | Policy date gaps and overlaps, negative premiums. |
| **[construction_project_logs.sql](construction_project_logs.sql)** | Construction | Circular dependencies in tasks, messy budget text fields. |
| **[restaurant_pos_variations.sql](restaurant_pos_variations.sql)** | Restaurant | Menu item name variations, tax calculation errors. |
| **[voting_machine_logs.sql](voting_machine_logs.sql)** | Government | Precinct ID variations, timestamp sorting, adjudicated vote flags. |
| **[fitness_tracker_sync.sql](fitness_tracker_sync.sql)** | Health | Timezone jumps during travel, sensor artifacts (>220 bpm). |
| **[warehouse_robot_telemetry.sql](warehouse_robot_telemetry.sql)** | IoT | Robot ID collisions (duplicates), battery level nulls/gaps. |

### Usage
Run these scripts to seed the "Bronze/Raw" layer in your Dremio environment. Then, ask your AI Agent:
> "Look at the `Old_Bank_System` folder. Inspect the data quality and propose a SQL view to clean it."
