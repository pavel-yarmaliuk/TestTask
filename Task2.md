# Task 2

## Overview

The GA4 raw export in BigQuery has a heavily nested schema (event_params repeated, items repeated, etc.). To make it
usable for reporting (e.g., in Looker Studio), we need to design a transformation layer that flattens, enriches, and
structures the data.

## Architecture

I propose two main tables to balance granularity and performance:

- ga4_events - One row per event, with key fields denormalised.
    - Grain: event‑level.
    - Columns: all top‑level fields (event_date, event_timestamp, event_name, user_pseudo_id, etc.) plus flattened event
      parameters, device, geo, traffic_source, and user properties.
    - Use case: detailed analysis, funnels, and event‑level segmentation.

- ga4_sessions - One row per session, aggregated from events.
    - Grain: session‑level (using reconstructed session ID).
    - Columns: session ID, user_pseudo_id, session start/end timestamps, session duration, number of events, first/last
      event names, campaign/source/medium, landing page, ecommerce metrics (purchases, revenue), etc.
    - Use case: session‑based metrics (bounce rate, conversions per session), marketing channel performance.

## Design Choices & Logic

### 1. Handling Nested & Repeated Fields

- event_params - Use `UNNEST` to extract key‑value pairs into columns. We only need a subset of parameters (e.g.,
  page_location, session_id, ga_session_number). Instead of creating one column per possible param (which is unbounded),
  we pivot only known parameters needed for reporting. Unknown parameters remain in a JSON column or are ignored.
- items - For ecommerce events, we typically flatten the first item or store items as JSON. For most reporting,
  aggregating revenue and quantity is sufficient, so we sum over items.

### 2. Session Reconstruction Logic

GA4 raw does not include a native session ID, but we can reconstruct it:

- Use event_params to get session_id (if present) - it is included in most events.
- If missing, fallback to time‑based grouping: for each user_pseudo_id, group events where the gap between consecutive
  timestamps is <= 30 minutes.
- The session_start event can be used to define session boundaries.
  In dbt, this can be implemented as:
- A staging model that extracts session_id from event_params.
- A session building model that uses LAG and TIMESTAMP_DIFF to assign session IDs to events without a native session_id.

### 3. Reporting‑Friendly Grain
- Wide table vs. multiple tables: I prefer separate events and sessions tables to avoid redundancy and maintain flexibility.
  - Events table is wide enough for most ad‑hoc queries and can be joined with sessions when session context is needed.
  - Sessions table aggregates facts that are easier to consume for dashboarding.
- Tradeoff: Some analyses might need both tables, increasing join complexity. However, this separation follows best practices for modular data modelling, reduces duplication, and simplifies incremental updates.
### 4. Assumptions (due to ambiguity)
- The raw data is the standard GA4 BigQuery export (version 3 or later). This ensures event_params structure is consistent.
- We assume we only need a predefined list of event parameters (e.g., page_location, session_id, ga_session_number) for reporting. If new parameters become important, the model can be extended.
- For session reconstruction, we assume the 30‑minute inactivity window is appropriate; this matches GA4’s default.
- The ecommerce columns in ecommerce and items are reliable and reflect the same data as event parameters (though often they are duplicates).

## Key Tradeoffs

| Decision                                  | Tradeoff                                                                            |
|-------------------------------------------|-------------------------------------------------------------------------------------|
| Separate events and sessions tables       | More flexibility but requires joins for session‑level queries                       |
| Extract only a subset of event parameters | Simpler schema, but new parameters require model updates                            |
| Use native session_id when available      | More accurate session boundaries, but may need fallback for older data without it   |
| Aggregate items into revenue/quantity     | Loses item‑level detail; for product‑level analysis, an items table might be needed |

## What Would Need to Change for Production
- Data volume - Use incremental models and partition/cluster tables (e.g., on event_date).
- Error handling - Add quality checks (dbt tests) and alerting for unexpected values (e.g., negative revenue).
- Schema changes - The GA4 schema occasionally adds new fields; we would monitor changes and update models with version‑controlled migrations.
- Performance - For very high volume, consider materialising intermediate tables and using incremental strategies that merge new data.