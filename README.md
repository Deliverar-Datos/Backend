# ETL Pipeline for Food Delivery Data Warehouse

## Overview
This ETL (Extract, Transform, Load) pipeline processes food delivery data from multiple sources and loads it into a PostgreSQL data warehouse with a star schema design. The pipeline extracts data from CSV files stored in AWS S3, transforms it into dimensional models, and loads it into a relational database for analytics and reporting.

## Features

- **Data Sources**: Processes data from multiple domains:
  - Customer data (users, wallets, orders)
  - Delivery data (drivers, assignments, locations)
  - Marketplace data (products, catalogs, promotions)
  - Blockchain data (transactions, wallets)

- **Incremental Loading**: Only processes new data since last run by tracking the most recent order date

- **Star Schema**: Creates dimensional models with:
  - Fact tables (orders, ratings)
  - Dimension tables (customers, products, drivers, dates, etc.)

- **Cloud Integration**: Works with AWS S3 for data storage

## Data Model

### Dimension Tables
- `dim_clientes`: Customer information
- `dim_wallets`: Customer wallets
- `dim_productos`: Product catalog
- `dim_repartidores`: Delivery drivers
- `dim_ubicaciones`: Driver locations
- `dim_sesiones`: Driver sessions
- `dim_transacciones`: Blockchain transactions
- `dim_wallets_blockchain`: Blockchain wallets
- `dim_tenants`: Marketplace tenants
- `dim_catalogos`: Product catalogs
- `dim_promociones`: Promotions
- `dim_imagenes_producto`: Product images
- `dim_datos_contacto`: Contact information
- `dim_fecha`: Date dimension
- `dim_ratings`: Customer ratings
- `dim_pedidos_delivery`: Delivery orders

### Fact Tables
- `fact_pedidos`: Order facts with product, customer, and driver information
- `fact_ratings`: Customer ratings facts

## Requirements

- Python 3.x
- Apache Spark
- PostgreSQL
- AWS credentials (for S3 access)
- Python packages:
  - pyspark
  - psycopg2-binary
  - boto3
  - python-dotenv

## Configuration

1. Create a `.env` file with the following variables:
   ```ini
   DATABASE_USER=your_db_user
   DATABASE_PASSWORD=your_db_password
   DATABASE_HOST=your_db_host
   DATABASE_NAME=your_db_name
   AWS_ACCESS_KEY_ID=your_aws_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret
   AWS_BUCKET_NAME=your_bucket_name
    ```
2. Place the PostgreSQL JDBC driver in the specified path

## Usage

1. Run the ETL pipeline:
   ```bash
   python3 main.py
   ```
2. For testing purposes, set the following:
    ```bash
   export IS_TEST=True
   ```
   
## Debugging

Set DEBUG = True in the ETL script to enable debug messages. The script will print confirmation when the ETL completes successfully.

## Incremental Processing

The pipeline automatically tracks the most recent processed order date and only loads new data in subsequent runs. This information is stored in the fact_pedidos table.

## Local Development

For local development, you can comment out the S3 file paths and uncomment the local file paths in the script.

## Support
For any issues or questions, please contact us.