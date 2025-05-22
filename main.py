import logging

import findspark
import os
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, lit, when, quarter, dayofweek, date_format
)
import boto3

from db_utils import write_to_db, get_postgres_connection, get_ultima_fecha_procesada, filtrar_nuevos
from spark import spark

conn = get_postgres_connection()
cursor = conn.cursor()


# Environment setup
DEBUG = True

findspark.init()

ultima_fecha = get_ultima_fecha_procesada()

# Read CSV with consistent options
def read_csv(path):
    return spark.read.option("header", True) \
        .option("inferSchema", True) \
        .option("multiLine", True) \
        .option("escape", '"') \
        .option("quote", '"') \
        .option("mode", "PERMISSIVE") \
        .csv(path)


# Write to PostgreSQL

# Load data from bucket in S3
bucket_name = os.environ.get("AWS_BUCKET_NAME")
s3 = boto3.client('s3', region_name="us-east-1")
prefix = "mock_data_comida/"

usuarios = read_csv(f"s3a://{bucket_name}/cliente_users.csv").withColumnRenamed("id", "cliente_id")
wallets = read_csv(f"s3a://{bucket_name}/cliente_wallets.csv")
productos_mkt = read_csv(f"s3a://{bucket_name}/marketplace_productos.csv")
pedidos = read_csv(f"s3a://{bucket_name}/cliente_pedidos.csv") \
    .withColumnRenamed("id", "pedido_id") \
    .withColumnRenamed("user", "cliente_id_pedido") \
    .withColumnRenamed("estado", "estado_pedido")
pedido_productos = read_csv(f"s3a://{bucket_name}/cliente_pedido_productos.csv")

asignaciones = read_csv(f"s3a://{bucket_name}/delivery_asignaciones.csv").withColumnRenamed("pedidoId",
                                                                                                    "pedido_id")
repartidores = read_csv(f"s3a://{bucket_name}/delivery_repartidores.csv").withColumnRenamed("id",
                                                                                                    "repartidor_id")
ubicaciones = read_csv(f"s3a://{bucket_name}/delivery_ubicaciones.csv")
sesiones = read_csv(f"s3a://{bucket_name}/delivery_sesiones.csv")
ratings = read_csv(f"s3a://{bucket_name}/delivery_ratings.csv")
delivery_pedidos = read_csv(f"s3a://{bucket_name}/delivery_pedidos.csv")

transacciones = read_csv(f"s3a://{bucket_name}/blockchain_transactions.csv")
blockchain_wallets = read_csv(f"s3a://{bucket_name}/blockchain_wallets.csv")

tenants = read_csv(f"s3a://{bucket_name}/marketplace_tenants.csv")
catalogos = read_csv(f"s3a://{bucket_name}/marketplace_catalogos.csv")
promociones = read_csv(f"s3a://{bucket_name}/marketplace_promociones.csv")
imagenes_producto = read_csv(f"s3a://{bucket_name}/marketplace_imagenes_producto.csv")
datos_contacto = read_csv(f"s3a://{bucket_name}/marketplace_datos_contacto.csv")

# # Local file system

# usuarios = read_csv("mock_data_comida/cliente/users.csv").withColumnRenamed("id", "cliente_id")
# wallets = read_csv("mock_data_comida/cliente/wallets.csv")
# productos_mkt = read_csv("mock_data_comida/marketplace/productos.csv")
# pedidos = read_csv("mock_data_comida/cliente/pedidos.csv") \
#     .withColumnRenamed("id", "pedido_id") \
#     .withColumnRenamed("user", "cliente_id_pedido") \
#     .withColumnRenamed("estado", "estado_pedido")
# pedido_productos = read_csv("mock_data_comida/cliente/pedido_productos.csv")
#
# asignaciones = read_csv("mock_data_comida/delivery/asignaciones.csv").withColumnRenamed("pedidoId", "pedido_id")
# repartidores = read_csv("mock_data_comida/delivery/repartidores.csv").withColumnRenamed("id", "repartidor_id")
# ubicaciones = read_csv("mock_data_comida/delivery/ubicaciones.csv")
# sesiones = read_csv("mock_data_comida/delivery/sesiones.csv")
# ratings = read_csv("mock_data_comida/delivery/ratings.csv")
# delivery_pedidos = read_csv("mock_data_comida/delivery/pedidos.csv")
#
# transacciones = read_csv("mock_data_comida/blockchain/transactions.csv")
# blockchain_wallets = read_csv("mock_data_comida/blockchain/wallets.csv")
#
# tenants = read_csv("mock_data_comida/marketplace/tenants.csv")
# catalogos = read_csv("mock_data_comida/marketplace/catalogos.csv")
# promociones = read_csv("mock_data_comida/marketplace/promociones.csv")
# imagenes_producto = read_csv("mock_data_comida/marketplace/imagenes_producto.csv")
# datos_contacto = read_csv("mock_data_comida/marketplace/datos_contacto.csv")

# DIMENSIONES
dim_clientes = usuarios.select("cliente_id", "nombre", "apellido", "email", "direccion", "telefono").dropDuplicates()
dim_wallets = wallets.withColumnRenamed("id", "cliente_id").dropDuplicates()
dim_productos = productos_mkt.select("producto_id", "catalogo_id", "nombre_producto", "categoria",
                                     "precio").dropDuplicates().withColumnRenamed("nombre_producto", "nombre")
dim_repartidores = repartidores.select("repartidor_id", "nombre", "estado").dropDuplicates()
dim_ubicaciones = ubicaciones.select("repartidorId", "latitud", "longitud", "timestamp").dropDuplicates()
dim_sesiones = sesiones.select("repartidorId", "fecha_inicio", "fecha_expiracion", "activa").dropDuplicates()
dim_transacciones = transacciones.select("id", "origin_user_id", "destination_user_id", "transaction_date",
                                         "currency_type", "amount", "concept").dropDuplicates()
dim_wallets_blockchain = blockchain_wallets.dropDuplicates()
dim_tenants = tenants.dropDuplicates()
dim_catalogos = catalogos.dropDuplicates()
dim_promociones = promociones.dropDuplicates()
dim_imagenes_producto = imagenes_producto.dropDuplicates()
dim_datos_contacto = datos_contacto.dropDuplicates()
dim_ratings = ratings.selectExpr("id as rating_id", "pedidoId as pedido_id", "repartidorId as repartidor_id", "puntaje",
                                 "comentario", "fecha_creacion").dropDuplicates()

# Fecha
dim_fecha = pedidos.select(to_date("fechaCreacion").alias("fecha")).dropDuplicates()
dim_fecha = dim_fecha \
    .withColumn("anio", year("fecha")) \
    .withColumn("mes", month("fecha")) \
    .withColumn("dia", dayofmonth("fecha")) \
    .withColumn("trimestre", quarter("fecha")) \
    .withColumn("nombre_mes", date_format("fecha", "MMMM")) \
    .withColumn("nombre_dia", date_format("fecha", "EEEE")) \
    .withColumn("numero_dia_semana", dayofweek("fecha"))
dim_pedidos_delivery = delivery_pedidos.dropDuplicates()

# HECHOS
hechos_pedidos = pedido_productos \
    .join(pedidos, "pedido_id") \
    .join(productos_mkt, "producto_id") \
    .join(usuarios, pedidos["cliente_id_pedido"] == usuarios["cliente_id"]) \
    .join(asignaciones, "pedido_id", "left") \
    .join(repartidores, repartidores["repartidor_id"] == asignaciones["repartidorId"], "left") \
    .withColumn("fecha_pedido", to_date("fechaCreacion")) \
    .withColumn("es_cripto", when(col("metodoPago") == "CRIPTO", lit(True)).otherwise(lit(False))) \
    .withColumn("total_producto", col("precio_unitario"))

fact_pedidos = hechos_pedidos.select(
    "pedido_id", "fecha_pedido",
    col("cliente_id").alias("cliente_id"),
    "producto_id", col("catalogo_id"),
    col("repartidorId").alias("repartidor_id"),
    "metodoPago", col("estado_pedido").alias("estado"),
    "es_cripto", "total_producto",
    pedidos["tenant_id"], pedidos["calificacion"], pedidos["comentario"], pedidos["tiempo_entrega_min"]
)

if ultima_fecha:
    logging.info(f"Filtrando pedidos desde la última fecha procesada: {ultima_fecha}")
    fact_pedidos = fact_pedidos.filter(col("fecha_pedido") > lit(ultima_fecha))
else:
    logging.info("No se encontró fecha de última carga, cargando todos los pedidos.")

fact_ratings = dim_ratings \
    .withColumnRenamed("id", "rating_id") \
    .withColumnRenamed("pedidoId", "pedido_id") \
    .withColumnRenamed("repartidorId", "repartidor_id")

fact_ratings = filtrar_nuevos(fact_ratings, "fact_ratings", "rating_id")
dim_clientes = filtrar_nuevos(dim_clientes, "dim_clientes", "cliente_id")
dim_productos = filtrar_nuevos(dim_productos, "dim_productos", "producto_id")
dim_repartidores = filtrar_nuevos(dim_repartidores, "dim_repartidores", "repartidor_id")
dim_ubicaciones = filtrar_nuevos(dim_ubicaciones, "dim_ubicaciones", "repartidorId")
dim_sesiones = filtrar_nuevos(dim_sesiones, "dim_sesiones", "repartidorId")
dim_transacciones = filtrar_nuevos(dim_transacciones, "dim_transacciones", "id")
dim_wallets_blockchain = filtrar_nuevos(dim_wallets_blockchain, "dim_wallets_blockchain", "id")
dim_tenants = filtrar_nuevos(dim_tenants, "dim_tenants", "tenant_id")
dim_catalogos = filtrar_nuevos(dim_catalogos, "dim_catalogos", "catalogo_id")
dim_promociones = filtrar_nuevos(dim_promociones, "dim_promociones", "promocion_id")
dim_imagenes_producto = filtrar_nuevos(dim_imagenes_producto, "dim_imagenes_producto", "imagen_id")
dim_datos_contacto = filtrar_nuevos(dim_datos_contacto, "dim_datos_contacto", "contacto_id")
dim_fecha = filtrar_nuevos(dim_fecha, "dim_fecha", "fecha")
dim_pedidos_delivery = filtrar_nuevos(dim_pedidos_delivery, "dim_pedidos_delivery", "pedido_id")


# Carga en la BDD
for df, name in [
    (dim_clientes, "dim_clientes"),
    (dim_wallets, "dim_wallets"),
    (dim_productos, "dim_productos"),
    (dim_repartidores, "dim_repartidores"),
    (dim_ubicaciones, "dim_ubicaciones"),
    (dim_sesiones, "dim_sesiones"),
    (dim_transacciones, "dim_transacciones"),
    (dim_wallets_blockchain, "dim_wallets_blockchain"),
    (dim_tenants, "dim_tenants"),
    (dim_catalogos, "dim_catalogos"),
    (dim_promociones, "dim_promociones"),
    (dim_imagenes_producto, "dim_imagenes_producto"),
    (dim_datos_contacto, "dim_datos_contacto"),
    (dim_fecha, "dim_fecha"),
    (dim_ratings, "dim_ratings"),
    (dim_pedidos_delivery, "dim_pedidos_delivery"),
    (fact_pedidos, "fact_pedidos"),
    (fact_ratings, "fact_ratings"),
]:
    write_to_db(df, name)

cursor.close()
conn.close()

if DEBUG:
    print("✅ ETL COMPLETO: Modelo estrella cargado correctamente en PostgreSQL y listo para Power BI.")
