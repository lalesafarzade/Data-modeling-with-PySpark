# =====================================================
# PART 1 - AWS GLUE SETUP AND READ RAW PARQUET FILES
# =====================================================

import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    current_timestamp,
    row_number,
    monotonically_increasing_id,
    year,
    month,
    quarter,
    dayofmonth,
    dayofweek,
    weekofyear,
    date_format,
    when,
    round
)

from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# -----------------------------------------------------
# Read Glue Parameters
# -----------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET"
    ]
)


# -----------------------------------------------------
# Initialize Spark / Glue
# -----------------------------------------------------

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# -----------------------------------------------------
# Logging
# -----------------------------------------------------

logger = logging.getLogger()

logger.setLevel(logging.INFO)

logger.info("===================================")
logger.info("Classic Models Warehouse ETL Started")
logger.info("===================================")


RAW_BUCKET = args["RAW_BUCKET"]


# -----------------------------------------------------
# Spark Configuration
# -----------------------------------------------------

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

spark.conf.set(
    "spark.sql.parquet.compression.codec",
    "snappy"
)

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "200"
)


# -----------------------------------------------------
# S3 Raw Paths
# -----------------------------------------------------

CUSTOMERS_PATH = (
    f"s3://{RAW_BUCKET}/raw/customers/"
)

EMPLOYEES_PATH = (
    f"s3://{RAW_BUCKET}/raw/employees/"
)

OFFICES_PATH = (
    f"s3://{RAW_BUCKET}/raw/offices/"
)

ORDERS_PATH = (
    f"s3://{RAW_BUCKET}/raw/orders/"
)

ORDER_DETAILS_PATH = (
    f"s3://{RAW_BUCKET}/raw/order_details/"
)

PRODUCTS_PATH = (
    f"s3://{RAW_BUCKET}/raw/products/"
)

PRODUCT_LINES_PATH = (
    f"s3://{RAW_BUCKET}/raw/product_lines/"
)

PAYMENTS_PATH = (
    f"s3://{RAW_BUCKET}/raw/payments/"
)


# -----------------------------------------------------
# Warehouse Output Paths
# -----------------------------------------------------

WAREHOUSE_PATH = (
    f"s3://{RAW_BUCKET}/warehouse"
)


DIM_CUSTOMER_PATH = (
    f"{WAREHOUSE_PATH}/dim_customer"
)

DIM_PRODUCT_PATH = (
    f"{WAREHOUSE_PATH}/dim_product"
)

DIM_EMPLOYEE_PATH = (
    f"{WAREHOUSE_PATH}/dim_employee"
)

DIM_OFFICE_PATH = (
    f"{WAREHOUSE_PATH}/dim_office"
)
logger.info(
    "All parquet files loaded successfully."
)
DIM_DATE_PATH = (
    f"{WAREHOUSE_PATH}/dim_date"
)

FACT_ORDER_PATH = (
    f"{WAREHOUSE_PATH}/fact_orders"
)

FACT_PAYMENT_PATH = (
    f"{WAREHOUSE_PATH}/fact_payments"
)


logger.info("S3 paths initialized.")


# -----------------------------------------------------
# Helper Functions
# -----------------------------------------------------

def log_count(df, name):

    count = df.count()

    logger.info(
        f"{name}: {count:,} rows"
    )

    return count



def clean_string(column_name):

    return upper(
        trim(
            col(column_name)
        )
    )



# -----------------------------------------------------
# Read Raw Parquet Files
# -----------------------------------------------------

logger.info("Reading raw parquet datasets...")


customers_df = spark.read.parquet(
    CUSTOMERS_PATH
)

employees_df = spark.read.parquet(
    EMPLOYEES_PATH
)

offices_df = spark.read.parquet(
    OFFICES_PATH
)

orders_df = spark.read.parquet(
    ORDERS_PATH
)

order_details_df = spark.read.parquet(
    ORDER_DETAILS_PATH
)

products_df = spark.read.parquet(
    PRODUCTS_PATH
)

product_lines_df = spark.read.parquet(
    PRODUCT_LINES_PATH
)

payments_df = spark.read.parquet(
    PAYMENTS_PATH
)


logger.info(
    "All parquet files loaded successfully."
)

customers_df = (
    customers_df
    .withColumnRenamed("customernumber", "customerNumber")
    .withColumnRenamed("customername", "customerName")
    .withColumnRenamed("contactfirstname", "contactFirstName")
    .withColumnRenamed("contactlastname", "contactLastName")
    .withColumnRenamed("addressline1", "addressLine1")
    .withColumnRenamed("addressline2", "addressLine2")
    .withColumnRenamed("postalcode", "postalCode")
    .withColumnRenamed("salesrepemployeenumber", "salesRepEmployeeNumber")
    .withColumnRenamed("creditlimit", "creditLimit")
)

employees_df = (
    employees_df
    .withColumnRenamed("employeenumber", "employeeNumber")
    .withColumnRenamed("firstname", "firstName")
    .withColumnRenamed("lastname", "lastName")
    .withColumnRenamed("officecode", "officeCode")
    .withColumnRenamed("reportsto", "reportsTo")
    .withColumnRenamed("jobtitle", "jobTitle")
)

offices_df = (
    offices_df
    .withColumnRenamed("officecode", "officeCode")
    .withColumnRenamed("postalcode", "postalCode")
)

orders_df = (
    orders_df
    .withColumnRenamed("ordernumber", "orderNumber")
    .withColumnRenamed("orderdate", "orderDate")
    .withColumnRenamed("requireddate", "requiredDate")
    .withColumnRenamed("shippeddate", "shippedDate")
    .withColumnRenamed("customernumber", "customerNumber")
)

order_details_df = (
    order_details_df
    .withColumnRenamed("ordernumber", "orderNumber")
    .withColumnRenamed("productcode", "productCode")
    .withColumnRenamed("quantityordered", "quantityOrdered")
    .withColumnRenamed("priceeach", "priceEach")
    .withColumnRenamed("orderlinenumber", "orderLineNumber")
)

products_df = (
    products_df
    .withColumnRenamed("productcode", "productCode")
    .withColumnRenamed("productname", "productName")
    .withColumnRenamed("productline", "productLine")
    .withColumnRenamed("productscale", "productScale")
    .withColumnRenamed("productvendor", "productVendor")
    .withColumnRenamed("productdescription", "productDescription")
    .withColumnRenamed("quantityinstock", "quantityInStock")
    .withColumnRenamed("buyprice", "buyPrice")
    .withColumnRenamed("msrp", "MSRP")
)

product_lines_df = (
    product_lines_df
    .withColumnRenamed("productline", "productLine")
    .withColumnRenamed("textdescription", "textDescription")
    .withColumnRenamed("htmldescription", "htmlDescription")
)


payments_df = (
    payments_df
    .withColumnRenamed("customernumber", "customerNumber")
    .withColumnRenamed("checknumber", "checkNumber")
    .withColumnRenamed("paymentdate", "paymentDate")
)
customers_df = customers_df.withColumn(
    "salesRepEmployeeNumber",
    col("salesRepEmployeeNumber").cast("long")
)

customers_df.printSchema()
employees_df.printSchema()
orders_df.printSchema()
products_df.printSchema()


# -----------------------------------------------------
# Initial Counts
# -----------------------------------------------------

log_count(
    customers_df,
    "Raw Customers"
)

log_count(
    employees_df,
    "Raw Employees"
)

log_count(
    offices_df,
    "Raw Offices"
)

log_count(
    orders_df,
    "Raw Orders"
)

log_count(
    order_details_df,
    "Raw Order Details"
)

log_count(
    products_df,
    "Raw Products"
)

log_count(
    product_lines_df,
    "Raw Product Lines"
)

log_count(
    payments_df,
    "Raw Payments"
)


logger.info("PART 1 COMPLETED")

# =====================================================
# PART 2 - DATA CLEANING AND PREPARATION
# =====================================================

logger.info("Starting data cleaning...")


# -----------------------------------------------------
# Remove Duplicate Records
# -----------------------------------------------------

customers_df = customers_df.dropDuplicates()

employees_df = employees_df.dropDuplicates()

offices_df = offices_df.dropDuplicates()

orders_df = orders_df.dropDuplicates()

order_details_df = order_details_df.dropDuplicates()

products_df = products_df.dropDuplicates()

product_lines_df = product_lines_df.dropDuplicates()

payments_df = payments_df.dropDuplicates()


logger.info(
    "Duplicate records removed."
)


# -----------------------------------------------------
# Clean Customers
# -----------------------------------------------------

customers_df = (
    customers_df

    .filter(
        col("customerNumber").isNotNull()
    )

    .withColumn(
        "customer_name",
        clean_string("customerName")
    )

    .withColumn(
        "customer_first_name",
        clean_string("contactFirstName")
    )

    .withColumn(
        "customer_last_name",
        clean_string("contactLastName")
    )

    .withColumn(
        "phone_clean",
        clean_string("phone")
    )

    .withColumn(
        "address_line_1",
        clean_string("addressLine1")
    )

    .withColumn(
        "address_line_2",
        clean_string("addressLine2")
    )

    .withColumn(
        "postal_code",
        clean_string("postalCode")
    )

    .withColumn(
        "city_clean",
        clean_string("city")
    )

    .withColumn(
        "state_clean",
        clean_string("state")
    )

    .withColumn(
        "country_clean",
        clean_string("country")
    )
)


# -----------------------------------------------------
# Clean Products
# -----------------------------------------------------

products_df = (
    products_df

    .filter(
        col("productCode").isNotNull()
    )

    .withColumn(
        "product_name",
        clean_string("productName")
    )

    .withColumn(
        "product_line",
        clean_string("productLine")
    )

    .withColumn(
        "product_scale",
        clean_string("productScale")
    )

    .withColumn(
        "product_vendor",
        clean_string("productVendor")
    )

    .withColumn(
        "product_description",
        clean_string("productDescription")
    )
)


# -----------------------------------------------------
# Clean Product Lines
# -----------------------------------------------------

product_lines_df = (
    product_lines_df

    .filter(
        col("productLine").isNotNull()
    )

    .withColumn(
        "product_line_clean",
        clean_string("productLine")
    )

    .withColumn(
        "product_line_description",
        clean_string("textDescription")
    )
)


# -----------------------------------------------------
# Clean Employees
# -----------------------------------------------------

employees_df = (
    employees_df

    .filter(
        col("employeeNumber").isNotNull()
    )

    .withColumn(
        "employee_first_name",
        clean_string("firstName")
    )

    .withColumn(
        "employee_last_name",
        clean_string("lastName")
    )

    .withColumn(
        "job_title_clean",
        clean_string("jobTitle")
    )

    .withColumn(
        "email_clean",
        clean_string("email")
    )
)


# -----------------------------------------------------
# Clean Offices
# -----------------------------------------------------

offices_df = (
    offices_df

    .filter(
        col("officeCode").isNotNull()
    )

    .withColumn(
        "city_clean",
        clean_string("city")
    )

    .withColumn(
        "state_clean",
        clean_string("state")
    )

    .withColumn(
        "country_clean",
        clean_string("country")
    )

    .withColumn(
        "postal_code",
        clean_string("postalCode")
    )

    .withColumn(
        "territory_clean",
        clean_string("territory")
    )
)


# -----------------------------------------------------
# Clean Orders
# -----------------------------------------------------

orders_df = (
    orders_df

    .filter(
        col("orderNumber").isNotNull()
    )

    .withColumn(
        "order_date",
        col("orderDate")
    )

    .withColumn(
        "required_date",
        col("requiredDate")
    )

    .withColumn(
        "shipped_date",
        col("shippedDate")
    )
)


# -----------------------------------------------------
# Clean Order Details
# -----------------------------------------------------

order_details_df = (
    order_details_df

    .filter(
        col("orderNumber").isNotNull()
    )

    .filter(
        col("productCode").isNotNull()
    )

)


# -----------------------------------------------------
# Clean Payments
# -----------------------------------------------------

payments_df = (
    payments_df

    .filter(
        col("customerNumber").isNotNull()
    )

    .withColumn(
        "payment_date",
        col("paymentDate")
    )

)


# -----------------------------------------------------
# Fill Missing Values
# -----------------------------------------------------

customers_df = customers_df.fillna(
    {
        "state_clean": "UNKNOWN",
        "city_clean": "UNKNOWN",
        "country_clean": "UNKNOWN"
    }
)


products_df = products_df.fillna(
    {
        "product_description": "UNKNOWN"
    }
)


employees_df = employees_df.fillna(
    {
        "job_title_clean": "UNKNOWN"
    }
)


offices_df = offices_df.fillna(
    {
        "state_clean": "UNKNOWN",
        "territory_clean": "UNKNOWN"
    }
)


# -----------------------------------------------------
# Join Products With Product Lines
# -----------------------------------------------------

products_enriched_df = (
    products_df.alias("p")

    .join(
        product_lines_df.alias("pl"),
        col("p.productLine")
        ==
        col("pl.productLine"),
        "left"
    )

    .select(
        col("p.*"),
        col("pl.product_line_description")
    )
)


# -----------------------------------------------------
# Add ETL Timestamp
# -----------------------------------------------------

customers_df = customers_df.withColumn(
    "etl_load_timestamp",
    current_timestamp()
)

products_enriched_df = products_enriched_df.withColumn(
    "etl_load_timestamp",
    current_timestamp()
)

employees_df = employees_df.withColumn(
    "etl_load_timestamp",
    current_timestamp()
)

offices_df = offices_df.withColumn(
    "etl_load_timestamp",
    current_timestamp()
)


logger.info(
    "Data cleaning completed."
)


# -----------------------------------------------------
# Final Counts
# -----------------------------------------------------

log_count(
    customers_df,
    "Clean Customers"
)

log_count(
    products_enriched_df,
    "Clean Products"
)

log_count(
    employees_df,
    "Clean Employees"
)

log_count(
    offices_df,
    "Clean Offices"
)

log_count(
    orders_df,
    "Clean Orders"
)

log_count(
    order_details_df,
    "Clean Order Details"
)

log_count(
    payments_df,
    "Clean Payments"
)


logger.info("PART 2 COMPLETED")

# =====================================================
# PART 3 - BUILD DIMENSIONS AND FACT TABLES
# =====================================================

logger.info("Building warehouse tables...")


# -----------------------------------------------------
# DIM CUSTOMER
# -----------------------------------------------------

logger.info("Creating dim_customer...")


customer_window = Window.orderBy(
    "customerNumber"
)


dim_customer = (
    customers_df

    .select(
        "customerNumber",
        "customer_name",
        "customer_first_name",
        "customer_last_name",
        "salesRepEmployeeNumber",
        "phone_clean",
        "address_line_1",
        "address_line_2",
        "postal_code",
        "city_clean",
        "state_clean",
        "country_clean",
        "creditLimit"
    )

    .dropDuplicates(
        ["customerNumber"]
    )

    .withColumn(
        "customer_key",
        row_number().over(customer_window)
    )

    .select(
        "customer_key",
        "customerNumber",
        "customer_name",
        "customer_first_name",
        "customer_last_name",
        "salesRepEmployeeNumber",
        "phone_clean",
        "address_line_1",
        "address_line_2",
        "postal_code",
        "city_clean",
        "state_clean",
        "country_clean",
        "creditLimit"
    )
)


log_count(
    dim_customer,
    "dim_customer"
)


# -----------------------------------------------------
# DIM PRODUCT
# -----------------------------------------------------

logger.info("Creating dim_product...")


product_window = Window.orderBy(
    "productCode"
)


dim_product = (
    products_enriched_df

    .select(
        "productCode",
        "product_name",
        "product_line",
        "product_scale",
        "product_vendor",
        "product_description",
        "product_line_description",
        "buyPrice",
        "MSRP"
    )

    .dropDuplicates(
        ["productCode"]
    )

    .withColumn(
        "product_key",
        row_number().over(product_window)
    )

    .select(
        "product_key",
        "productCode",
        "product_name",
        "product_line",
        "product_scale",
        "product_vendor",
        "product_description",
        "product_line_description",
        "buyPrice",
        "MSRP"
    )
)


log_count(
    dim_product,
    "dim_product"
)


# -----------------------------------------------------
# DIM OFFICE
# -----------------------------------------------------

logger.info("Creating dim_office...")


office_window = Window.orderBy(
    "officeCode"
)


dim_office = (
    offices_df

    .select(
        "officeCode",
        "city_clean",
        "state_clean",
        "country_clean",
        "postal_code",
        "territory_clean"
    )

    .dropDuplicates(
        ["officeCode"]
    )

    .withColumn(
        "office_key",
        row_number().over(office_window)
    )

    .select(
        "office_key",
        "officeCode",
        "city_clean",
        "state_clean",
        "country_clean",
        "postal_code",
        "territory_clean"
    )
)


log_count(
    dim_office,
    "dim_office"
)


# -----------------------------------------------------
# DIM EMPLOYEE
# -----------------------------------------------------

logger.info("Creating dim_employee...")


employee_window = Window.orderBy(
    "employeeNumber"
)


dim_employee = (
    employees_df

    .join(
        dim_office,
        "officeCode",
        "left"
    )

    .select(
        "employeeNumber",
        "employee_first_name",
        "employee_last_name",
        "job_title_clean",
        "email_clean",
        "office_key"
    )

    .dropDuplicates(
        ["employeeNumber"]
    )

    .withColumn(
        "employee_key",
        row_number().over(employee_window)
    )

    .select(
        "employee_key",
        "employeeNumber",
        "employee_first_name",
        "employee_last_name",
        "job_title_clean",
        "email_clean",
        "office_key"
    )
)


log_count(
    dim_employee,
    "dim_employee"
)


# -----------------------------------------------------
# DIM DATE
# -----------------------------------------------------

logger.info("Creating dim_date...")


date_window = Window.orderBy(
    "order_date"
)


dim_date = (
    orders_df

    .select(
        "order_date"
    )

    .distinct()

    .withColumn(
        "date_key",
        row_number().over(date_window)
    )

    .withColumn(
        "year",
        year("order_date")
    )

    .withColumn(
        "quarter",
        quarter("order_date")
    )

    .withColumn(
        "month",
        month("order_date")
    )

    .withColumn(
        "day",
        dayofmonth("order_date")
    )

    .withColumn(
        "week",
        weekofyear("order_date")
    )

    .withColumn(
        "day_name",
        date_format(
            "order_date",
            "EEEE"
        )
    )

    .withColumn(
        "month_name",
        date_format(
            "order_date",
            "MMMM"
        )
    )
)


log_count(
    dim_date,
    "dim_date"
)


# -----------------------------------------------------
# FACT ORDERS
# -----------------------------------------------------

logger.info("Creating fact_orders...")


fact_window = Window.orderBy(
    "orderNumber",
    "orderLineNumber"
)


fact_orders = (

    orders_df.alias("o")

    .join(
        order_details_df.alias("od"),
        col("o.orderNumber")
        ==
        col("od.orderNumber"),
        "inner"
    )

    .join(
        dim_customer.alias("dc"),
        col("o.customerNumber")
        ==
        col("dc.customerNumber"),
        "inner"
    )

    .join(
        dim_product.alias("dp"),
        col("od.productCode")
        ==
        col("dp.productCode"),
        "inner"
    )

    .join(
        dim_employee.alias("de"),
        col("dc.salesRepEmployeeNumber")
        ==
        col("de.employeeNumber"),
        "left"
    )

    .join(
        dim_date.alias("dd"),
        col("o.order_date")
        ==
        col("dd.order_date"),
        "inner"
    )

    .select(
        "o.orderNumber",
        "od.orderLineNumber",

        "dc.customer_key",
        "dp.product_key",
        "de.employee_key",
        "dd.date_key",

        col("od.quantityOrdered")
        .alias("quantity_ordered"),

        col("od.priceEach")
        .alias("product_price"),

        (
            col("od.quantityOrdered")
            *
            col("od.priceEach")
        )
        .alias("sales_amount"),

        col("o.order_date"),
        col("o.required_date"),
        col("o.shipped_date")
    )

    .withColumn(
        "fact_order_key",
        row_number().over(fact_window)
    )

    .withColumn(
        "etl_load_timestamp",
        current_timestamp()
    )
)


log_count(
    fact_orders,
    "fact_orders"
)


# -----------------------------------------------------
# FACT PAYMENTS
# -----------------------------------------------------

logger.info("Creating fact_payments...")


payment_window = Window.orderBy(
   "customer_key",
    "checkNumber"
)


fact_payments = (

    payments_df.alias("p")

    .join(
        dim_customer.alias("c"),
        col("p.customerNumber")
        ==
        col("c.customerNumber"),
        "inner"
    )

    .select(
        col("c.customer_key"),

        col("p.checkNumber"),

        col("p.payment_date"),

        col("p.amount")
    )

    .withColumn(
        "payment_key",
        row_number().over(payment_window)
    )

    .withColumn(
        "etl_load_timestamp",
        current_timestamp()
    )
)


log_count(
    fact_payments,
    "fact_payments"
)


# -----------------------------------------------------
# Cache Warehouse Tables
# -----------------------------------------------------

dim_customer.cache()

dim_product.cache()

dim_employee.cache()

dim_office.cache()

dim_date.cache()

fact_orders.cache()

fact_payments.cache()


logger.info(
    "PART 3 COMPLETED"
)

# =====================================================
# PART 4 - WRITE WAREHOUSE PARQUET FILES
# =====================================================

logger.info("Writing warehouse tables to S3...")


# -----------------------------------------------------
# Write Dimension Tables
# -----------------------------------------------------

(
    dim_customer
    .write
    .mode("overwrite")
    .format("parquet")
    .option(
        "compression",
        "snappy"
    )
    .save(
        DIM_CUSTOMER_PATH
    )
)


logger.info(
    "dim_customer written."
)


(
    dim_product
    .write
    .mode("overwrite")
    .format("parquet")
    .option(
        "compression",
        "snappy"
    )
    .save(
        DIM_PRODUCT_PATH
    )
)


logger.info(
    "dim_product written."
)


(
    dim_employee
    .write
    .mode("overwrite")
    .format("parquet")
    .option(
        "compression",
        "snappy"
    )
    .save(
        DIM_EMPLOYEE_PATH
    )
)


logger.info(
    "dim_employee written."
)


(
    dim_office
    .write
    .mode("overwrite")
    .format("parquet")
    .option(
        "compression",
        "snappy"
    )
    .save(
        DIM_OFFICE_PATH
    )
)


logger.info(
    "dim_office written."
)


(
    dim_date
    .write
    .mode("overwrite")
    .format("parquet")
    .option(
        "compression",
        "snappy"
    )
    .save(
        DIM_DATE_PATH
    )
)


logger.info(
    "dim_date written."
)


# -----------------------------------------------------
# Write Fact Tables
# -----------------------------------------------------

(
    fact_orders
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy(
        "order_date"
    )
    .option(
        "compression",
        "snappy"
    )
    .save(
        FACT_ORDER_PATH
    )
)


logger.info(
    "fact_orders written."
)



(
    fact_payments
    .write
    .mode("overwrite")
    .format("parquet")
    .option(
        "compression",
        "snappy"
    )
    .save(
        FACT_PAYMENT_PATH
    )
)


logger.info(
    "fact_payments written."
)



# =====================================================
# VALIDATE WRITTEN DATA
# =====================================================

logger.info(
    "Validating warehouse output..."
)


warehouse_dim_customer = spark.read.parquet(
    DIM_CUSTOMER_PATH
)

warehouse_dim_product = spark.read.parquet(
    DIM_PRODUCT_PATH
)

warehouse_dim_employee = spark.read.parquet(
    DIM_EMPLOYEE_PATH
)

warehouse_dim_office = spark.read.parquet(
    DIM_OFFICE_PATH
)

warehouse_dim_date = spark.read.parquet(
    DIM_DATE_PATH
)

warehouse_fact_orders = spark.read.parquet(
    FACT_ORDER_PATH
)

warehouse_fact_payments = spark.read.parquet(
    FACT_PAYMENT_PATH
)



log_count(
    warehouse_dim_customer,
    "Warehouse dim_customer"
)

log_count(
    warehouse_dim_product,
    "Warehouse dim_product"
)

log_count(
    warehouse_dim_employee,
    "Warehouse dim_employee"
)

log_count(
    warehouse_dim_office,
    "Warehouse dim_office"
)

log_count(
    warehouse_dim_date,
    "Warehouse dim_date"
)

log_count(
    warehouse_fact_orders,
    "Warehouse fact_orders"
)

log_count(
    warehouse_fact_payments,
    "Warehouse fact_payments"
)



# -----------------------------------------------------
# Print Schemas
# -----------------------------------------------------

logger.info(
    "Warehouse Schemas"
)


warehouse_dim_customer.printSchema()

warehouse_dim_product.printSchema()

warehouse_dim_employee.printSchema()

warehouse_dim_office.printSchema()

warehouse_dim_date.printSchema()

warehouse_fact_orders.printSchema()

warehouse_fact_payments.printSchema()



# =====================================================
# CLEANUP CACHE
# =====================================================

logger.info(
    "Clearing cache..."
)


dim_customer.unpersist()

dim_product.unpersist()

dim_employee.unpersist()

dim_office.unpersist()

dim_date.unpersist()

fact_orders.unpersist()

fact_payments.unpersist()



# =====================================================
# JOB SUMMARY
# =====================================================

logger.info(
    "==================================="
)

logger.info(
    "Classic Models Warehouse Completed"
)

logger.info(
    "==================================="
)


logger.info(
    f"dim_customer: {DIM_CUSTOMER_PATH}"
)

logger.info(
    f"dim_product: {DIM_PRODUCT_PATH}"
)

logger.info(
    f"dim_employee: {DIM_EMPLOYEE_PATH}"
)

logger.info(
    f"dim_office: {DIM_OFFICE_PATH}"
)

logger.info(
    f"dim_date: {DIM_DATE_PATH}"
)

logger.info(
    f"fact_orders: {FACT_ORDER_PATH}"
)

logger.info(
    f"fact_payments: {FACT_PAYMENT_PATH}"
)



# -----------------------------------------------------
# Commit AWS Glue Job
# -----------------------------------------------------

job.commit()


logger.info(
    "Glue job committed successfully."
)