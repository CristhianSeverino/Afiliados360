import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

# 1. Bloque de inicialización de Glue
# Este código es obligatorio para que el script se ejecute correctamente en el entorno de AWS Glue.
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Lógica del Job (tu código de ETL)
# Leer tablas desde el Glue Data Catalog
dyf_usuarios = glueContext.create_dynamic_frame.from_catalog(
    database="360-afiliados",
    table_name="usuarios2024",
    transformation_ctx="dyf_usuarios"
)
dyf_servicios = glueContext.create_dynamic_frame.from_catalog(
    database="360-afiliados",
    table_name="servicios_usados",
    transformation_ctx="dyf_servicios"
)
dyf_subsidios = glueContext.create_dynamic_frame.from_catalog(
    database="360-afiliados",
    table_name="subsidios_otorgados",
    transformation_ctx="dyf_subsidios"
)

# Convertir DynamicFrames a DataFrames
df_usuarios = dyf_usuarios.toDF()
df_servicios = dyf_servicios.toDF()
df_subsidios = dyf_subsidios.toDF()

# Asegurar tipos de datos
df_usuarios = df_usuarios.withColumn("id", col("id").cast("string"))
df_servicios = df_servicios.withColumn("id", col("id").cast("string"))
df_subsidios = df_subsidios.withColumn("id", col("id").cast("string"))
df_subsidios = df_subsidios.withColumn("monto", col("monto").cast("int"))

# Realizar el join
df_joined = df_usuarios.join(
    df_servicios,
    on="id",
    how="inner"
).join(
    df_subsidios,
    on="id",
    how="inner"
)

# Transformación y selección de columnas
df_transformed = df_joined.select(
    df_usuarios["id"], 
    col("categoria"),
    col("servicio"),
    col("municipio"),
    col("monto").alias("total_subsidio")
)

# Convertir a DynamicFrame para escritura
dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")

# Escribir resultado en S3
glueContext.write_dynamic_frame.from_options(
    frame=dyf_transformed,
    connection_type="s3",
    connection_options={"path": "s3://transform-360-afiliados/Glue/processed-zone/joined_data/"},
    format="parquet",
    transformation_ctx="write_parquet"
)

# Commit del job
job.commit()
