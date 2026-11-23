from pathlib import Path
from databricks.connect import DatabricksSession
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).with_name(".env"))

# host = "https://xxx-xxxxxxxx-xxxx.cloud.databricks.com/"
# token = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
host = os.getenv("host")
token = os.getenv("token")

spark = (
    DatabricksSession.builder
        .host(host)
        .token(token)
        .serverless()
        .getOrCreate()
)

df = spark.read.table("dev.spark_db.diamonds")
df.show(5)
