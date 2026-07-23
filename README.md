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

Technology	Purpose
Amazon S3	Data storage layer
AWS Glue	Serverless ETL processing
Apache Spark	Distributed data processing
PySpark	Data transformation framework
Parquet	Columnar storage format
IAM	Security and permissions

