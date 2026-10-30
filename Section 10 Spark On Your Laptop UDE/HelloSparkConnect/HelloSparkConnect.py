from databricks.connect import DatabricksSession
import os

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

spark = (
    DatabricksSession.builder
        .host(host)
        .token(token)
        .serverless()
        .getOrCreate()
)

df = spark.read.table("dev.spark_db.diamonds")
df.show(5)
