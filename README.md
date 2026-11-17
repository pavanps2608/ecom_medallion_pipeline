E-commerce Medallion Pipeline (Databricks)

An end-to-end batch data pipeline on Databricks using the medallion architecture (Bronze → Silver → Gold), Unity Catalog (catalog, schemas, volumes) and PySpark. It ingests raw e-commerce orders and product master JSON files, applies data quality rules, and produces curated and aggregated Delta tables.

Architecture
Unity Catalog Volumes (raw JSON)
  source_1/load_date=YYYY-MM-DD/orders.json
  source_2/load_date=YYYY-MM-DD/products.json
        │
        ▼
  BRONZE  (raw + ingestion metadata)
  ecom_medallion_arc_pipeline.bronze.orders_bronze
  ecom_medallion_arc_pipeline.bronze.products_bronze
        │
        ▼
  SILVER  (validated, deduplicated, enriched)
  ecom_medallion_arc_pipeline.silver.orders_curated
  ecom_medallion_arc_pipeline.silver.rejected_orders
        │
        ▼
  GOLD    (business aggregates)
  ecom_medallion_arc_pipeline.gold.daily_product_sales
  ecom_medallion_arc_pipeline.gold.category_sales
  
Repository structure

File	Layer	Purpose
01_source_to_bronze.py	Bronze	Reads raw JSON from volumes, adds metadata, appends to Delta tables
02_bronze_to_silver.py	Silver	Data quality checks, rejects, product dedup, order enrichment
03_silver_to_gold.py	Gold	Builds daily product sales and category sales aggregates
data/orders.json	Sample source	Sample orders data
data/products.json	Sample source	Sample product master data (includes intentionally bad records)

Unity Catalog setup
Object	Name
Catalog	ecom_medallion_arc_pipeline
Schemas	bronze, silver, gold, data_layer
Volumes	ecom_medallion_arc_pipeline.data_layer.source_1 (orders), ecom_medallion_arc_pipeline.data_layer.source_2 (products)

Raw files are stored under a Hive-style partition folder, e.g. /Volumes/ecom_medallion_arc_pipeline/data_layer/source_1/load_date=2026-04-03/.

Pipeline stages
1. Source → Bronze (01_source_to_bronze.py)
   
Reads multi-line JSON from the volume paths.
Adds metadata columns: source_file_name (from _metadata.file_path, since input_file_name() is unsupported on Unity Catalog), ingestion_timestamp, pipeline_run_id, and load_date (regex-extracted from the folder path).
Writes to Delta in append mode, partitioned by load_date.

3. Bronze → Silver (02_bronze_to_silver.py)
   
Orders
Converts blank strings to NULL, standardizes status (lower/trim), casts quantity and unit price, and parses order_date in both yyyy-MM-dd HH:mm:ss and yyyy-MM-dd'T'HH:mm:ss formats.
Flags duplicate order_ids (first occurrence kept by load_date, file name, ingestion time).

Rejection reasons (first matching rule wins): NULL_ORDER_ID, NULL_CUSTOMER_ID, NULL_PRODUCT_ID, INVALID_ORDER_DATE, NULL_OR_INVALID_QUANTITY, INVALID_QUANTITY, NULL_OR_INVALID_UNIT_PRICE, INVALID_UNIT_PRICE, INVALID_ORDER_STATUS (allowed: completed, pending, cancelled), DUPLICATE_ORDER_ID.

Rejected rows go to silver.rejected_orders with the original record as JSON (raw_record), reason, source file, load date and run id.

Products
Drops records with a null product_id, product_name, category or brand, and deduplicates on product_id.

Enrichment

Valid orders are left-joined to the product master; missing matches default to UNKNOWN. order_amount = quantity * unit_price is derived.
Both Silver tables are written in overwrite mode.

3. Silver → Gold (03_silver_to_gold.py)
Applies not-null safety filters, then builds:

gold.daily_product_sales: grain order_date + product_id + product_name + category
gold.category_sales: grain order_date + category

Running the pipeline

Create the catalog, schemas and volumes listed above.
Upload the source files into the volume folders using the load_date=YYYY-MM-DD/ convention.
Import the three scripts as Databricks notebooks.
Run them in order (01 → 02 → 03) manually, or orchestrate them as a Databricks Job with three dependent tasks.
Each notebook prints row counts and previews at the end for validation.

Tech stack
Databricks · Unity Catalog (catalog, schemas, volumes) · PySpark · Delta Lake · Databricks Jobs
