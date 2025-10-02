STEDI Lakehouse Project
=======================

Overview
--------

The STEDI Lakehouse Project implements a **data lakehouse architecture in AWS** to support the STEDI Step Trainer initiative. The STEDI Step Trainer is a device that collects balance and motion data through IoT sensors, paired with a mobile app. This project aims to build a pipeline that processes this data, ensuring it is **trusted, curated, and privacy-compliant**, ultimately creating a dataset ready for training **machine learning models** to detect user steps in real-time.

The solution leverages **AWS Glue, AWS S3, Athena, Spark, and Python** to ingest, clean, validate, and integrate data from multiple landing zones into a structured, queryable, and analytics-ready format.

* * *

Objectives
----------

-   Ingest raw sensor, customer, and accelerometer data into S3 landing zones.
    
-   Apply data quality checks and **filter customers who consented to research**.
    
-   Create **trusted** and **curated** datasets using AWS Glue ETL jobs.
    
-   Resolve **data quality issues** related to duplicate serial numbers in fulfillment records.
    
-   Produce a **machine\_learning\_curated dataset** that combines Step Trainer IoT and accelerometer readings for downstream ML models.
    

Architecture
------------

**Landing Zones (Raw Data)**

-   `customer_landing`
    
-   `accelerometer_landing`
    
-   `step_trainer_landing`
    

**Trusted Zones (Cleaned Data)**

-   `customer_trusted` → Only includes customers who consented to share data.
    
-   `accelerometer_trusted` → Contains accelerometer readings from consenting customers.
    
-   `step_trainer_trusted` → Includes Step Trainer readings linked to curated customers.
    

**Curated Zone (Final Outputs)**

-   `customers_curated` → Customers with valid serial numbers and accelerometer
    
    data.
    
-   `machine_learning_curated` → Combined Step Trainer and accelerometer readings aligned by timestamp, intended for ML training.
    

Tools & Technologies
--------------------

-   **AWS S3** → Data lake storage (landing, trusted, curated zones).
    
-   **AWS Glue (PySpark ETL jobs)** → Data cleaning, filtering, and integration.
    
-   **AWS Athena** → Querying and validating datasets.
    
-   **Python & PySpark** → Data transformations, joins, and timestamp alignment.
    
-   **Spark SQL** → Aggregation and schema management.
    

Project Workflow
----------------

1.  **Data Ingestion**
    
    -   Uploaded raw CSV files into S3 landing zones.
        
    -   Created Glue/Athena external tables for each landing dataset.
        
2.  **Sanitization & Trusted Zone Creation**
    
    -   Filtered `customer_landing` to create `customer_trusted` (consent = TRUE).
        
    -   Filtered `accelerometer_landing` to create `accelerometer_trusted` (only consenting customers).
        
3.  **Curated Zone Creation**
    
    -   Built `customers_curated` to include customers with valid serial numbers and accelerometer data.
        
    -   Built `step_trainer_trusted` to include only Step Trainer records linked to curated customers.
        
4.  **Machine Learning Dataset**
    
    -   Joined Step Trainer and accelerometer readings by timestamp to produce `machine_learning_curated`.
        
    -   Ensured a high-quality, research-compliant dataset for ML model development.
        

* * *

Data Validation & Quality Assurance
-----------------------------------

-   Ensured only customers with `share_research = TRUE` were included.
    
-   Verified that distinct serial numbers from Step Trainer IoT data matched curated customers.
    
-   Reconciled row counts before and after filtering at each stage.
    
-   Queried with Athena to validate schema and sample records.
    

* * *

Results
-------

-   Delivered a fully operational **AWS Lakehouse pipeline** with clearly defined landing, trusted, and curated zones.
    
-   Produced a **machine\_learning\_curated dataset** containing synchronized accelerometer and Step Trainer IoT records.
    
-   Created reusable Glue ETL jobs and SQL scripts for reproducibility.
    
-   Ensured compliance with **privacy requirements** by excluding non-consenting users.
