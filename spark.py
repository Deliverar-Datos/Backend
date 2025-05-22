import logging

from pyspark.sql import SparkSession

import config


def get_spark_session():
    spark_session = SparkSession.builder \
        .appName("ETL DELIVER AR") \
        .config("spark.driver.extraClassPath", config.POSTGRES_JAR_PATH) \
        .getOrCreate()
    logging.info("Spark session created successfully")
    return spark_session


spark = get_spark_session()