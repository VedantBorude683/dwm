# Retail Sales Data Mart - Working Pipeline

Requirements: Python 3.8+ (standard library only, nothing to install).

Run:
    python retail_datamart_pipeline.py                 # full pipeline
    python retail_datamart_pipeline.py --regenerate --rows 1000
    python retail_datamart_pipeline.py --source your_file.csv
    python retail_datamart_pipeline.py --query "SELECT category, SUM(sales_amount) FROM v_sales GROUP BY category"

Pipeline: sales_source.csv -> EXTRACT (stg_sales) -> TRANSFORM (validate/clean/derive, rejects logged)
-> LOAD (dim_date, dim_product, dim_store, dim_customer, fact_sales) -> VERIFY (reconciliation, no orphans)
-> CUBE (16 cuboids in cube_sales) -> OLAP (roll-up, drill-down, slice, dice, pivot) -> reports/*.csv

Files: sales_datamart.db (open in DB Browser for SQLite), reports/ (one CSV per query), etl_rejects table (data-quality log).
Your own source CSV needs these columns: txn_id, txn_date (DD-MM-YYYY), product_code, product_name, category, brand,
store_code, store_name, city, region, cust_id, cust_name, segment, qty, unit_price, discount
