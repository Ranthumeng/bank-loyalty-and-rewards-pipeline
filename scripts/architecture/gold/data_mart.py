from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.getOrCreate()

# ==========================================
# CONFIGURATION
# ==========================================
SOURCE_SILVER_TABLE  = "workspace.default.silver_layer"
CUSTOMER_LEVEL_TABLE = "workspace.default.greenbacks_customer_level"

GOLD_CUSTOMER_SUMMARY_TABLE   = "workspace.default.gold_customer_points_summary"
GOLD_MONTHLY_TREND_TABLE      = "workspace.default.gold_monthly_points_trend"
GOLD_CATEGORY_BREAKDOWN_TABLE = "workspace.default.gold_category_breakdown"
GOLD_CATEGORY_LEADERBOARD_TABLE = "workspace.default.gold_category_leaderboard"

# Set to True (or env var "true") when you want to fully reset gold tables before rebuilding (for testing)

RESET_GOLD_TABLES = os.getenv("RESET_GOLD_TABLES", "false").lower() == "true"

#  1. Every gold table here is an aggregate over a customer's *entire* history
#     (lifetime totals, monthly trends, category breakdowns) -- a single new or
#     corrected transaction can change numbers for that customer going back to
#     their very first transaction, so there's no meaningful "only touch the new
#     rows" version of these tables, the same reason the silver cumulative step
#     isn't incremental either.
#  2. Silver is already the validated source of truth (its own pipeline asserts
#     `cumulative_points_earned` is exact before it finishes). Gold should be a
#     thin, cheap aggregation layer on top of that guarantee, not a place that
#     re-derives business logic (earn rates, eligibility, GB_VALUE_RAND) a second
#     time. Every column below is either a straight aggregate of an
#     already-computed silver column, or a "latest known value" lookup.


_silver_row_count = spark.table(SOURCE_SILVER_TABLE).count()
if _silver_row_count == 0:
    raise ValueError(
        f"{SOURCE_SILVER_TABLE} is empty. Run the silver layer pipeline first -- "
        f"there is nothing for the gold layer to aggregate yet."
    )
print(f"[INFO] Building gold layer from {_silver_row_count} row(s) in {SOURCE_SILVER_TABLE}")


# --- Reset clause for testing ---
if RESET_GOLD_TABLES:
    print("[INFO] RESET_GOLD_TABLES=True: dropping existing gold tables before rebuild")
    for tbl in [
        GOLD_CUSTOMER_SUMMARY_TABLE,
        GOLD_MONTHLY_TREND_TABLE,
        GOLD_CATEGORY_BREAKDOWN_TABLE,
        GOLD_CATEGORY_LEADERBOARD_TABLE,
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")


df_silver = spark.table(SOURCE_SILVER_TABLE)


_approved_spend = (F.col("transaction_type") == "SPEND") & (F.col("status") == "APPROVED")



#  ## gold_customer_points_summary
#  Grain: one row per `customer_id`. This is the "account balance" view -- what you'd
#  show on a customer's rewards dashboard or use to drive a redemption flow.


_latest_attrs_window = Window.partitionBy("customer_id").orderBy(F.col("timestamp").desc())


df_latest_customer_attrs = (
    df_silver
    .withColumn("_rn", F.row_number().over(_latest_attrs_window))
    .filter(F.col("_rn") == 1)
    .select("customer_id", "home_town", "lifestyle_archetype", "living_tier")
)


df_customer_agg = df_silver.groupBy("customer_id").agg(
    F.round(
        F.sum(F.when(_approved_spend, F.col("amount")).otherwise(0.0)), 2
    ).alias("lifetime_spend_rand"),
    F.max("cumulative_points_earned").alias("lifetime_points_earned"),
    F.round(
        F.max("cumulative_points_value_rand"), 2
    ).alias("lifetime_points_value_rand"),
    F.sum(F.when(_approved_spend, 1).otherwise(0)).alias("approved_spend_transactions"),
    F.count(F.lit(1)).alias("total_transactions"),
    F.min("timestamp").alias("first_transaction_at"),
    F.max("timestamp").alias("last_transaction_at"),
)

# Optional validation: ensure all living_tier values have a match in CUSTOMER_LEVEL_TABLE
_missing_tiers = (
    df_silver
    .filter(F.col("living_tier").isNotNull())
    .select("living_tier").distinct()
    .join(spark.table(CUSTOMER_LEVEL_TABLE), on="living_tier", how="left_anti")
)


if _missing_tiers.count() > 0:
    print("[WARN] Some living_tier values have no match in CUSTOMER_LEVEL_TABLE:")
    _missing_tiers.show(truncate=False)


df_customer_summary = (
    df_customer_agg
    .join(df_latest_customer_attrs, on="customer_id", how="left")
    .join(F.broadcast(spark.table(CUSTOMER_LEVEL_TABLE)), on="living_tier", how="left")
    .withColumn("gold_refreshed_at", F.current_timestamp())
    .select(
        "customer_id",
        "home_town",
        "lifestyle_archetype",
        "living_tier",
        "level",  
        "lifetime_spend_rand",
        "lifetime_points_earned",
        "lifetime_points_value_rand",
        "approved_spend_transactions",
        "total_transactions",
        "first_transaction_at",
        "last_transaction_at",
        "gold_refreshed_at",
    )
)


(
    df_customer_summary.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_CUSTOMER_SUMMARY_TABLE)
)
print(f"[INFO] Rebuilt {GOLD_CUSTOMER_SUMMARY_TABLE}: {df_customer_summary.count()} customer(s)")


#  ## gold_monthly_points_trend


df_monthly = (
    df_silver
    .withColumn("txn_month", F.date_trunc("month", F.col("timestamp")).cast("date"))
    .groupBy("customer_id", "txn_month")
    .agg(
        F.sum("points_earned").alias("points_earned_in_month"),
        F.round(F.sum("points_value_rand"), 2).alias("points_value_rand_in_month"),
        F.round(
            F.sum(F.when(_approved_spend, F.col("amount")).otherwise(0.0)), 2
        ).alias("spend_in_month_rand"),
        F.sum(F.when(_approved_spend, 1).otherwise(0)).alias("approved_spend_transactions"),
    )
    .withColumn("gold_refreshed_at", F.current_timestamp())
)


(
    df_monthly.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_MONTHLY_TREND_TABLE)
)
print(f"[INFO] Rebuilt {GOLD_MONTHLY_TREND_TABLE}: {df_monthly.count()} customer-month row(s)")


#  ## gold_category_breakdown
#  useful for personalised marketing (e.g. "you could earn more by spending at bp").
#  Restricted to approved spend only, since declined/pending transactions and
#  non-spend transaction types never earn points and would just add zero-value noise
#  to every category row.


df_category = (
    df_silver
    .filter(_approved_spend)
    .groupBy("customer_id", "category")
    .agg(
        F.round(F.sum("amount"), 2).alias("spend_in_category_rand"),
        F.sum("points_earned").alias("points_earned_in_category"),
        F.round(F.sum("points_value_rand"), 2).alias("points_value_rand_in_category"),
        F.count(F.lit(1)).alias("transaction_count"),
    )
    .withColumn("gold_refreshed_at", F.current_timestamp())
)


(
    df_category.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_CATEGORY_BREAKDOWN_TABLE)
)
print(f"[INFO] Rebuilt {GOLD_CATEGORY_BREAKDOWN_TABLE}: {df_category.count()} customer-category row(s)")


#  ## gold_category_leaderboard
#  Grain: `category` only, across all customers. This is the exec-dashboard view --
#  "which spend categories are driving the most Greenbacks payout overall" -- reusing
#  the per-customer category breakdown above rather than re-aggregating silver a
#  third time.


df_category_leaderboard = (
    df_category
    .groupBy("category")
    .agg(
        F.round(F.sum("spend_in_category_rand"), 2).alias("total_spend_rand"),
        F.sum("points_earned_in_category").alias("total_points_earned"),
        F.round(F.sum("points_value_rand_in_category"), 2).alias("total_points_value_rand"),
        F.countDistinct("customer_id").alias("distinct_customers"),
        F.sum("transaction_count").alias("transaction_count"),
    )
    .withColumn("gold_refreshed_at", F.current_timestamp())
    .orderBy(F.col("total_points_value_rand").desc())
)


(
    df_category_leaderboard.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_CATEGORY_LEADERBOARD_TABLE)
)
print(f"[INFO] Rebuilt {GOLD_CATEGORY_LEADERBOARD_TABLE}: {df_category_leaderboard.count()} category row(s)")


#  ## Cross-check gold against silver's validated cumulative totals
#  Silver already proves `cumulative_points_earned` is an exact running total before
#  its own pipeline finishes. This step proves gold didn't lose or duplicate
#  anything on the way out: `lifetime_points_earned` in the customer summary must
#  exactly equal the sum of that customer's `points_earned_in_month` rows in the
#  monthly trend table. If it doesn't, gold has drifted from silver and this fails
#  loudly rather than shipping a dashboard that disagrees with itself.


df_monthly_rollup = (
    spark.table(GOLD_MONTHLY_TREND_TABLE)
    .groupBy("customer_id")
    .agg(F.sum("points_earned_in_month").alias("rollup_points_earned"))
)


df_consistency_check = (
    spark.table(GOLD_CUSTOMER_SUMMARY_TABLE)
    .join(df_monthly_rollup, on="customer_id", how="left")
    .filter(F.col("lifetime_points_earned") != F.col("rollup_points_earned"))
)


_mismatch_count = df_consistency_check.count()
if _mismatch_count > 0:
    print(f"[ERROR] gold_customer_points_summary and gold_monthly_points_trend disagree "
          f"for {_mismatch_count} customer(s):")
    df_consistency_check.select(
        "customer_id", "lifetime_points_earned", "rollup_points_earned"
    ).show(truncate=False)
    raise AssertionError(
        f"Gold layer internal consistency check failed for {_mismatch_count} customer(s). "
        f"Aborting rather than leaving disagreeing gold tables in place."
    )
else:
    print(f"[INFO] Gold layer validated: customer summary totals match the monthly trend "
          f"roll-up for every customer.")
