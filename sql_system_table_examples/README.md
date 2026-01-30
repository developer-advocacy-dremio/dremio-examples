# System Table Examples

This directory contains queries for inspecting Dremio's internal system tables for monitoring and auditing.

## Files

- **[job_analytics.sql](job_analytics.sql)**: Analyzing job performance and resource usage via `sys.jobs`.
- **[reflection_status.sql](reflection_status.sql)**: Monitoring the health and refresh status of reflections using `sys.reflections`.
- **[membership_and_privileges.sql](membership_and_privileges.sql)**: Auditing user membership and role privileges via `sys.privileges` and `sys.membership`.
