import time
import pandas as pd
import boto3
import streamlit as st

REGION = "us-east-1"
DATABASE = "globalpartners_curated"
ATHENA_OUTPUT = "s3://globalpartners-niteesh-us-east-1-2026/athena-results/"  # must match Athena settings
WORKGROUP = "primary"  # change if you used a different workgroup

athena = boto3.client("athena", region_name=REGION)

def run_athena_query(sql: str) -> pd.DataFrame:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=WORKGROUP,
    )
    qid = resp["QueryExecutionId"]

    # wait
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)
        state = q["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = q["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        raise RuntimeError(f"Athena query {state}: {reason}")

    # fetch results (paginated)
    paginator = athena.get_paginator("get_query_results")
    rows = []
    cols = None

    for page in paginator.paginate(QueryExecutionId=qid):
        result_set = page["ResultSet"]
        if cols is None:
            cols = [c["VarCharValue"] for c in result_set["Rows"][0]["Data"]]
            data_rows = result_set["Rows"][1:]
        else:
            data_rows = result_set["Rows"]

        for r in data_rows:
            rows.append([d.get("VarCharValue") for d in r["Data"]])

    df = pd.DataFrame(rows, columns=cols)
    return df

st.set_page_config(page_title="GlobalPartners Dashboard", layout="wide")
st.title("GlobalPartners | Customer & Sales Analytics")

as_of_date = st.date_input("As-of date", value=pd.to_datetime("2026-03-13").date())

# ---------------------------
# KPI Summary
# ---------------------------
kpi_sql = f"""
SELECT
  COUNT(*) AS orders,
  SUM(net_order_revenue) AS revenue,
  AVG(net_order_revenue) AS aov
FROM fact_order_revenue
WHERE as_of_date = DATE '{as_of_date}'
"""
kpi = run_athena_query(kpi_sql)

c1, c2, c3 = st.columns(3)
c1.metric("Orders", int(float(kpi.loc[0, "orders"])))
c2.metric("Revenue", f"${float(kpi.loc[0, 'revenue']):,.2f}")
c3.metric("AOV", f"${float(kpi.loc[0, 'aov']):,.2f}")

st.divider()

# ---------------------------
# RFM Segments
# ---------------------------
seg_sql = f"""
SELECT segment, COUNT(*) AS customers
FROM customer_rfm_daily
WHERE as_of_date = DATE '{as_of_date}'
GROUP BY 1
ORDER BY customers DESC
"""
seg = run_athena_query(seg_sql)
seg["customers"] = seg["customers"].astype(int)

left, right = st.columns(2)
with left:
    st.subheader("RFM Segment Distribution")
    st.dataframe(seg, use_container_width=True)

with right:
    st.subheader("Churn Risk (simple list)")
    churn_sql = f"""
    SELECT user_id, recency_days, frequency_90d, monetary_90d
    FROM customer_rfm_daily
    WHERE as_of_date = DATE '{as_of_date}'
      AND segment = 'Churn_Risk'
    ORDER BY recency_days DESC
    LIMIT 50
    """
    churn = run_athena_query(churn_sql)
    st.dataframe(churn, use_container_width=True)

st.divider()

# ---------------------------
# Location Performance
# ---------------------------
st.subheader("Location Performance (Top 25 by revenue)")
loc_sql = f"""
SELECT restaurant_id, revenue_to_date, orders_to_date, aov_to_date, loyalty_order_share, discounted_orders_to_date
FROM location_daily_perf
WHERE as_of_date = DATE '{as_of_date}'
ORDER BY revenue_to_date DESC
LIMIT 25
"""
loc = run_athena_query(loc_sql)
st.dataframe(loc, use_container_width=True)

st.divider()

# ---------------------------
# Discount Effectiveness
# ---------------------------
st.subheader("Discount Effectiveness")
disc_sql = f"""
SELECT
  has_discount,
  COUNT(*) AS orders,
  SUM(net_order_revenue) AS revenue,
  AVG(net_order_revenue) AS aov
FROM fact_order_revenue
WHERE as_of_date = DATE '{as_of_date}'
GROUP BY has_discount
ORDER BY has_discount DESC
"""
disc = run_athena_query(disc_sql)
st.dataframe(disc, use_container_width=True)
