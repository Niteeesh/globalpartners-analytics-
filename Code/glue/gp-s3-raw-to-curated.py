import sys
from datetime import datetime, date
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# -----------------------------
# Args (BUCKET + EXTRACT_DATE)
# -----------------------------
arg_keys = ['JOB_NAME']
# Optional args
for k in ['BUCKET', 'EXTRACT_DATE']:
    if f'--{k}' in sys.argv:
        arg_keys.append(k)

args = getResolvedOptions(sys.argv, arg_keys)

BUCKET = args.get('BUCKET', 'globalpartners-niteesh-us-east-1-2026')
EXTRACT_DATE = args.get('EXTRACT_DATE', datetime.utcnow().strftime('%Y-%m-%d'))  # e.g., 2026-03-05
AS_OF_DATE = EXTRACT_DATE  # single-day snapshot; later we can backfill range

RAW_BASE = f"s3://{BUCKET}/raw"
CURATED_BASE = f"s3://{BUCKET}/curated"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def read_raw(table_prefix: str):
    path = f"{RAW_BASE}/{table_prefix}/extract_date={EXTRACT_DATE}/"
    return spark.read.parquet(path)

# -----------------------------
# 1) Read raw tables
# -----------------------------
items = read_raw("order_items")
options = read_raw("order_item_options")
date_dim = read_raw("date_dim")

# Basic hygiene: keep only rows with required keys
items = items.filter(F.col("order_id").isNotNull() & F.col("lineitem_id").isNotNull())
options = options.filter(F.col("order_id").isNotNull() & F.col("lineitem_id").isNotNull())

# Normalize types
items = (
    items
    .withColumn("creation_ts", F.to_timestamp("creation_time_utc"))
    .withColumn("order_date", F.to_date("creation_ts"))
    .withColumn("item_revenue", (F.col("item_price") * F.col("item_quantity")).cast("double"))
)

options = (
    options
    .withColumn("option_revenue", (F.col("option_price") * F.col("option_quantity")).cast("double"))
    .withColumn("is_discount_option", F.when(F.col("option_price") < 0, F.lit(1)).otherwise(F.lit(0)))
)

as_of_date_lit = F.to_date(F.lit(AS_OF_DATE))

# -----------------------------
# 2) FACT: order-level revenue
# -----------------------------
# Item revenue per order
items_order = (
    items.groupBy("order_id")
    .agg(
        F.first("user_id", ignorenulls=True).alias("user_id"),
        F.first("restaurant_id", ignorenulls=True).alias("restaurant_id"),
        F.min("order_date").alias("order_date"),
        F.max(F.col("is_loyalty").cast("int")).alias("order_loyalty_flag"),
        F.sum(F.coalesce("item_revenue", F.lit(0.0))).alias("gross_item_revenue"),
        F.countDistinct("lineitem_id").alias("lineitems_count")
    )
)

# Options revenue per order
options_order = (
    options.groupBy("order_id")
    .agg(
        F.sum(F.coalesce("option_revenue", F.lit(0.0))).alias("option_revenue"),
        F.max("is_discount_option").alias("has_discount")
    )
)

fact_order_revenue = (
    items_order.join(options_order, on="order_id", how="left")
    .fillna({"option_revenue": 0.0, "has_discount": 0})
    .withColumn("net_order_revenue", (F.col("gross_item_revenue") + F.col("option_revenue")).cast("double"))
    .withColumn("as_of_date", as_of_date_lit)
)

# Write fact_order_revenue
fact_out = f"{CURATED_BASE}/facts/fact_order_revenue"
(
    fact_order_revenue
    .write.mode("overwrite")
    .partitionBy("as_of_date")
    .parquet(fact_out)
)

# -----------------------------
# 3) CUSTOMER DAILY SNAPSHOT
# -----------------------------
# Only orders on/before as_of_date
orders_upto = fact_order_revenue.filter(F.col("order_date") <= as_of_date_lit)

customer_base = (
    orders_upto.groupBy("user_id")
    .agg(
        F.sum("net_order_revenue").alias("ltv_to_date"),
        F.countDistinct("order_id").alias("orders_to_date"),
        F.max("order_date").alias("last_order_date"),
        F.sum(F.when(F.col("order_loyalty_flag") == 1, 1).otherwise(0)).alias("loyal_orders_to_date"),
    )
    .withColumn("as_of_date", as_of_date_lit)
    .withColumn("days_since_last_order", F.datediff(as_of_date_lit, F.col("last_order_date")))
)

# Rolling windows (30/60/90) based on order_date
def rolling_agg(days: int, prefix: str):
    return (
        fact_order_revenue
        .filter((F.col("order_date") <= as_of_date_lit) & (F.col("order_date") >= F.date_sub(as_of_date_lit, days)))
        .groupBy("user_id")
        .agg(
            F.sum("net_order_revenue").alias(f"spend_{prefix}"),
            F.countDistinct("order_id").alias(f"orders_{prefix}")
        )
    )

roll_30 = rolling_agg(30, "last_30d")
roll_60 = rolling_agg(60, "last_60d")
roll_90 = rolling_agg(90, "last_90d")

customer_daily_snapshot = (
    customer_base
    .join(roll_30, on="user_id", how="left")
    .join(roll_60, on="user_id", how="left")
    .join(roll_90, on="user_id", how="left")
    .fillna({
        "spend_last_30d": 0.0, "orders_last_30d": 0,
        "spend_last_60d": 0.0, "orders_last_60d": 0,
        "spend_last_90d": 0.0, "orders_last_90d": 0,
    })
    .withColumn("loyalty_flag", F.when(F.col("loyal_orders_to_date") > 0, F.lit(1)).otherwise(F.lit(0)))
)

snap_out = f"{CURATED_BASE}/snapshots/customer_daily_snapshot"
(
    customer_daily_snapshot
    .write.mode("overwrite")
    .partitionBy("as_of_date")
    .parquet(snap_out)
)

# -----------------------------
# 4) CUSTOMER RFM DAILY (scores + segments)
# -----------------------------
# Define R, F, M metrics (90d window)
rfm_base = (
    customer_daily_snapshot
    .select(
        "user_id", "as_of_date",
        F.col("days_since_last_order").alias("recency_days"),
        F.col("orders_last_90d").alias("frequency_90d"),
        F.col("spend_last_90d").alias("monetary_90d"),
        "loyalty_flag"
    )
)

# Compute quantile cutoffs (approx)
# For recency lower is better; for frequency/monetary higher is better.
rec_q = rfm_base.select("recency_days").na.drop().approxQuantile("recency_days", [0.2, 0.4, 0.6, 0.8], 0.01)
freq_q = rfm_base.select("frequency_90d").na.drop().approxQuantile("frequency_90d", [0.2, 0.4, 0.6, 0.8], 0.01)
mon_q = rfm_base.select("monetary_90d").na.drop().approxQuantile("monetary_90d", [0.2, 0.4, 0.6, 0.8], 0.01)

def score_high_better(col, q):
    return (
        F.when(col <= F.lit(q[0]), 1)
         .when(col <= F.lit(q[1]), 2)
         .when(col <= F.lit(q[2]), 3)
         .when(col <= F.lit(q[3]), 4)
         .otherwise(5)
    )

def score_low_better(col, q):
    # Invert: lower recency_days => higher score
    return (
        F.when(col <= F.lit(q[0]), 5)
         .when(col <= F.lit(q[1]), 4)
         .when(col <= F.lit(q[2]), 3)
         .when(col <= F.lit(q[3]), 2)
         .otherwise(1)
    )

rfm_scored = (
    rfm_base
    .withColumn("r_score", score_low_better(F.col("recency_days"), rec_q))
    .withColumn("f_score", score_high_better(F.col("frequency_90d"), freq_q))
    .withColumn("m_score", score_high_better(F.col("monetary_90d"), mon_q))
)

# Simple segments (you can refine later)
rfm_segmented = (
    rfm_scored
    .withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), F.lit("VIP"))
         .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), F.lit("Churn_Risk"))
         .when((F.col("f_score") <= 2) & (F.col("r_score") >= 4), F.lit("New_or_Returning"))
         .otherwise(F.lit("Regular"))
    )
)

rfm_out = f"{CURATED_BASE}/snapshots/customer_rfm_daily"
(
    rfm_segmented
    .write.mode("overwrite")
    .partitionBy("as_of_date")
    .parquet(rfm_out)
)

# -----------------------------
# 5) LOCATION DAILY PERFORMANCE
# -----------------------------
location_daily_perf = (
    fact_order_revenue
    .filter(F.col("order_date") <= as_of_date_lit)
    .groupBy("restaurant_id", "as_of_date")
    .agg(
        F.sum("net_order_revenue").alias("revenue_to_date"),
        F.countDistinct("order_id").alias("orders_to_date"),
        F.avg("net_order_revenue").alias("aov_to_date"),
        F.avg(F.col("order_loyalty_flag").cast("double")).alias("loyalty_order_share"),
        F.sum(F.col("has_discount").cast("int")).alias("discounted_orders_to_date")
    )
)

loc_out = f"{CURATED_BASE}/facts/location_daily_perf"
(
    location_daily_perf
    .write.mode("overwrite")
    .partitionBy("as_of_date")
    .parquet(loc_out)
)

# -----------------------------
# 6) Write a run log
# -----------------------------
run_log_rows = [
    ("fact_order_revenue", fact_out, AS_OF_DATE),
    ("customer_daily_snapshot", snap_out, AS_OF_DATE),
    ("customer_rfm_daily", rfm_out, AS_OF_DATE),
    ("location_daily_perf", loc_out, AS_OF_DATE),
]
run_log_df = spark.createDataFrame(run_log_rows, ["dataset", "path", "as_of_date"])
run_log_path = f"s3://{BUCKET}/logs/glue/gp-s3-raw-to-curated/as_of_date={AS_OF_DATE}/"
run_log_df.write.mode("overwrite").parquet(run_log_path)

print("Curated layer build completed successfully.")
job.commit()
