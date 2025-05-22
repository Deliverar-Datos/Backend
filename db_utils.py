import logging

import psycopg2

import config
from spark import spark

url = f"jdbc:postgresql://{config.DATABASE_HOST}:5432/{config.DATABASE_NAME}"



def get_postgres_connection():
    return psycopg2.connect(
        dbname=config.DATABASE_NAME,
        user=config.DATABASE_USER,
        password=config.DATABASE_PASSWORD,
        host=config.DATABASE_HOST,
        port="5432"
    )


def write_to_db(df, table_name):
    df.write.format("jdbc") \
        .option("url", url) \
        .option("user", config.DATABASE_USER) \
        .option("password", config.DATABASE_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()


def reset_schema():
    try:
        conn = psycopg2.connect(
            dbname=config.DATABASE_NAME,
            user=config.DATABASE_USER,
            password=config.DATABASE_PASSWORD,
            host=config.DATABASE_HOST,
            port="5432"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.info(f"Error al resetear el esquema: {e}")


from pyspark.sql.functions import max


def get_ultima_fecha_procesada():
    try:
        df_dw = spark.read.format("jdbc") \
            .option("url", url) \
            .option("user", config.DATABASE_USER) \
            .option("password", config.DATABASE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "fact_pedidos") \
            .load()

        if df_dw.rdd.isEmpty():
            logging.info("fact_pedidos está vacío.")
            return None

        return df_dw.select(max("fecha_pedido")).collect()[0][0]

    except Exception as e:
        logging.error(f"Error consultando última fecha en fact_pedidos: {e}")
        return None


def filtrar_nuevos(df_nuevo, tabla_dw, campo_pk):
    df_existente = spark.read.format("jdbc") \
        .option("url", url) \
        .option("user", config.DATABASE_USER) \
        .option("password", config.DATABASE_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", tabla_dw) \
        .load() \
        .select(campo_pk).dropDuplicates()

    return df_nuevo.join(df_existente, campo_pk, "anti")
