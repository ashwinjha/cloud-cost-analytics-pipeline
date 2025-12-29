# Cloud Cost Analytics Pipeline (Spark – Local Simulation)


## Problem Statement

This project simulates a cloud billing analytics pipeline that computes **daily total cost of services used per project** across all accounts and events.

The pipeline is designed to answer questions such as:
- How much did each service cost per project per day?  
- How do corrected or late-arriving billing events impact historical cost reports?     

The focus is on demonstrating **production-style data engineering fundamentals**:
immutable raw ingestion, late data handling, deduplication, daily aggregation,
backfills, and data quality checks — using a minimal local setup.

---------------------------------------------------------------------------------------

## Why Cost Analytics?

Cost analytics was chosen because it naturally exercises core data engineering
concerns:

- Billing data often arrives late or out of order
- Previously reported usage can be corrected
- Historical recomputation must be deterministic for audits

This makes cost data a good neutral domain to demonstrate
**idempotent pipelines and backfill handling**
without relying on proprietary or company-specific datasets.

---------------------------------------------------------------------------------------

## Input Data (Raw Events)

Simulated billing events with the following schema: 

event_id   
event_date  
account_id  
service_name  
cost_usd  
ingestion_date  



### Characteristics intentionally simulated
- Duplicate events  
- Corrected costs (same `event_id`, later `ingestion_date`)  
- Late arrivals for historical dates  

Raw data is treated as **immutable**.

---------------------------------------------------------------------------------------

## Processing Logic

### Raw Ingestion
- Raw CSV events are read without modification  
- No deduplication at this stage  

### Deduplication
- Deduplicated by `event_id`  
- Latest record selected using `ingestion_date` (latest-wins)  

### Aggregation
Daily grain aggregation:

```text
event_date + service_name + project_id
```


Metric:

```text
total_cost_usd
```


This results in a curated dataset that represents
**daily service-level cost per project**.

### Backfill Handling
- Late and corrected events are reprocessed using the same logic  
- Pipeline is **idempotent** and **deterministic**

---------------------------------------------------------------------------------------

## Data Quality Checks

The pipeline fails fast if:
- Critical columns contain nulls  
- Aggregated output is empty  
- Negative costs are detected  

---------------------------------------------------------------------------------------

## Example Analytical Queries

The curated daily dataset can be used to answer questions such as:

```sql
-- Daily cost by service and project
SELECT
  event_date,
  service_name,
  project_id,
  total_cost_usd
FROM daily_service_cost
ORDER BY event_date, total_cost_usd DESC;  

-- Total cost per service over a time range
SELECT
  service_name,
  SUM(total_cost_usd) AS service_cost
FROM daily_service_cost
WHERE event_date BETWEEN '2024-01-01' AND '2024-01-07'
GROUP BY service_name;  

-- Cost trends by service
SELECT
  service_name,
  event_date,
  total_cost_usd
FROM daily_service_cost
ORDER BY service_name, event_date;
```
These queries illustrate how the aggregated data supports -   
cost analysis, trend monitoring, and downstream reporting.   


---------------------------------------------------------------------------------------


## Architecture Overview  

## Architecture Overview


```text
Raw CSV Events
      |
      v
Spark (PySpark)
  - Deduplication (latest ingestion wins)
  - Daily aggregation
  - Data quality checks
      |
      v
Curated Daily Cost Dataset
```


-------------------------------------------------------------------------------------



## Technology
- PySpark (Spark SQL / DataFrame APIs)  
- Local execution (no cloud dependencies)  

**Note:**  
- Raw ingestion and curated transformations were implemented using Spark DataFrames.     
- For local Windows development, transformations were validated in-memory due to filesystem constraints.    
- The same logic is **format-agnostic** and runs on Parquet in production environments.    
- Pipeline correctness was validated via deterministic recomputation, backfill reruns, and data quality checks.  


------------------------------------------------------------------------------------

## Key Takeaways
- Immutable raw data  
- Deterministic recomputation  
- Late data and backfill support  
- Production-oriented Spark transformations
  
------------------------------------------------------------------------------------
  


