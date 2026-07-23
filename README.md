# Data-modeling-with-PySpark

Project Overview
This project builds a cloud-based data warehouse pipeline using AWS Glue, Apache Spark (PySpark), and Amazon S3.

The pipeline reads raw Classic Models business data stored as Parquet files in Amazon S3, performs data cleaning and transformation, creates dimensional warehouse tables (star schema), and writes optimized Parquet datasets back to S3.

The goal of this project is to demonstrate an end-to-end ETL workflow using serverless AWS data engineering technologies.

# Architecture
                 +----------------+
                 |  Source Data   |
                 |  Parquet Files |
                 +-------+--------+
                         |
                         |
                         v
              +--------------------+
              |      Amazon S3     |
              |       raw/         |
              +---------+----------+
                        |
                        |
                        v
              +--------------------+
              |     AWS Glue Job   |
              |      PySpark ETL   |
              +---------+----------+
                        |
                        |
        +---------------+---------------+
        |                               |
        v                               v
+---------------+              +----------------+
| Data Cleaning |              | Data Modeling  |
| Transformation|              | Star Schema    |
+---------------+              +----------------+
                                        |
                                        |
                                        v
                              +----------------+
                              | Amazon S3      |
                              | warehouse/     |
                              +----------------+

# Technologies Used

Technology	
Amazon S3	
AWS Glue	
Apache Spark	
PySpark	Data 
Parquet	
IAM	

# Project Structure
pyspark-demo-lala/

│
├── script/
│   └── classical_modeles_warehouse.py
│
├── raw/
│   ├── customers/
│   ├── employees/
│   ├── offices/
│   ├── orders/
│   ├── order_details/
│   ├── products/
│   ├── product_lines/
│   └── payments/
│
└── warehouse/
    ├── dim_customer/
    ├── dim_product/
    ├── dim_employee/
    ├── dim_office/
    ├── dim_date/
    ├── fact_orders/
    └── fact_payments/


