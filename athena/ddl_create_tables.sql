CREATE DATABASE IF NOT EXISTS globalpartners_curated;

# fact_order_revenue
CREATE EXTERNAL TABLE IF NOT EXISTS globalpartners_curated.fact_order_revenue (
  order_id            bigint,
  user_id             string,
  restaurant_id       string,
  order_date          date,
  order_loyalty_flag  int,
  gross_item_revenue  double,
  lineitems_count     bigint,
  option_revenue      double,
  has_discount        int,
  net_order_revenue   double
)
PARTITIONED BY (as_of_date date)
STORED AS PARQUET
LOCATION 's3://globalpartners-niteesh-us-east-1-2026/curated/facts/fact_order_revenue/';

# customer_daily_snapshot
CREATE EXTERNAL TABLE IF NOT EXISTS globalpartners_curated.customer_daily_snapshot (
  user_id               string,
  ltv_to_date            double,
  orders_to_date         bigint,
  last_order_date        date,
  loyal_orders_to_date   bigint,
  days_since_last_order  int,
  spend_last_30d         double,
  orders_last_30d        bigint,
  spend_last_60d         double,
  orders_last_60d        bigint,
  spend_last_90d         double,
  orders_last_90d        bigint,
  loyalty_flag           int
)
PARTITIONED BY (as_of_date date)
STORED AS PARQUET
LOCATION 's3://globalpartners-niteesh-us-east-1-2026/curated/snapshots/customer_daily_snapshot/';


# customer_rfm_daily
CREATE EXTERNAL TABLE IF NOT EXISTS globalpartners_curated.customer_rfm_daily (
  user_id         string,
  recency_days    int,
  frequency_90d   bigint,
  monetary_90d    double,
  loyalty_flag    int,
  r_score         int,
  f_score         int,
  m_score         int,
  segment         string
)
PARTITIONED BY (as_of_date date)
STORED AS PARQUET
LOCATION 's3://globalpartners-niteesh-us-east-1-2026/curated/snapshots/customer_rfm_daily/';

# location_daily_perf
CREATE EXTERNAL TABLE IF NOT EXISTS globalpartners_curated.location_daily_perf (
  restaurant_id             string,
  revenue_to_date           double,
  orders_to_date            bigint,
  aov_to_date               double,
  loyalty_order_share       double,
  discounted_orders_to_date bigint
)
PARTITIONED BY (as_of_date date)
STORED AS PARQUET
LOCATION 's3://globalpartners-niteesh-us-east-1-2026/curated/facts/location_daily_perf/';
