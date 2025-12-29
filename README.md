# Cloud Cost Analytics Pipeline (Spark – Local Simulation)

## Problem Statement

This project simulates a simplified cloud billing analytics pipeline to demonstrate
**end-to-end data engineering fundamentals**:

- Immutable raw ingestion  
- Late data handling  
- Deduplication  
- Daily aggregation  
- Backfills  
- Data quality checks  

The goal is to validate **production-style thinking** using a minimal local setup.

---------------------------------------------------------------------------------------

## Input Data (Raw Events)

Simulated billing events with the following schema: 

event_id 
event_date 
account_id 
project_id 
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

event_date + service_name + project_id

Metric:

total_cost_usd


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

## Technology
- PySpark (Spark SQL / DataFrame APIs)  
- Local execution (no cloud dependencies)  

**Note:**  
Raw ingestion and curated transformations were implemented using Spark DataFrames.
For local Windows development, transformations were validated in-memory due to filesystem constraints.  
The same logic is **format-agnostic** and runs on Parquet in production environments. 
Pipeline correctness was validated via deterministic recomputation, backfill reruns, and data quality checks.


------------------------------------------------------------------------------------

## Key Takeaways
- Immutable raw data  
- Deterministic recomputation  
- Late data and backfill support  
- Production-oriented Spark transformations
  
------------------------------------------------------------------------------------
  


