import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET", "JDBC_URL", "JDBC_USER", "JDBC_PASSWORD"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["BUCKET"]
JDBC_URL = args["JDBC_URL"]
JDBC_USER = args["JDBC_USER"]
JDBC_PASSWORD = args["JDBC_PASSWORD"]

EXTRACT_DATE = datetime.utcnow().strftime("%Y-%m-%d")

TABLES = [
    ("dbo.order_items", "raw/order_items"),
    ("dbo.order_item_options", "raw/order_item_options"),
    ("dbo.date_dim", "raw/date_dim"),
]

def read_table_jdbc(table_name: str):
    # Use dbtable for simple reads; Spark will infer schema from SQL Server
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

log_rows = []

for src_table, target_prefix in TABLES:
    print(f"Reading {src_table} via Spark JDBC ...")
    df = read_table_jdbc(src_table)

    print(f"{src_table} columns = {df.columns}")
    row_count = df.count()
    print(f"{src_table} row_count = {row_count}")

    df_out = (
        df.withColumn("extract_date", lit(EXTRACT_DATE))
          .withColumn("extracted_at_utc", current_timestamp())
    )

    target_path = f"s3://{BUCKET}/{target_prefix}/extract_date={EXTRACT_DATE}/"
    print(f"Writing {src_table} to {target_path}")

    df_out.write.mode("overwrite").parquet(target_path)

    log_rows.append((src_table, row_count, target_path, EXTRACT_DATE))

log_df = spark.createDataFrame(log_rows, ["source_table", "row_count", "target_path", "extract_date"])
log_path = f"s3://{BUCKET}/logs/glue/gp-rds-to-s3-raw/extract_date={EXTRACT_DATE}/"
log_df.write.mode("overwrite").parquet(log_path)

print("Extraction completed successfully.")
job.commit()
