from pyspark.sql import functions as F

TARGET_TABLE = "catalog_name.schema_name.table_name"  # silver table

CONFIG_TABLE            = "catalog_name.schema_name.greenbacks_config"
LEVEL_EARN_RATES_TABLE  = "catalog_name.schema_name.greenbacks_level_earn_rates"
CUSTOMER_LEVEL_TABLE    = "catalog_name.schema_name.greenbacks_customer_level"
CATEGORY_RULES_TABLE    = "catalog_name.schema_name.greenbacks_category_rules"
MERCHANT_OVERRIDES_TABLE = "catalog_name.schema_name.greenbacks_merchant_overrides"

# ==========================================
# WATERMARK: only pull bronze rows landed since the last successful merge
# ==========================================
WATERMARK_SAFETY_BUFFER = "INTERVAL 1 HOUR"

if spark.catalog.tableExists(TARGET_TABLE):
    watermark = spark.sql(f"SELECT MAX(ingestion_time) AS watermark FROM {TARGET_TABLE}").collect()[0]["watermark"]
else:
    watermark = None

df = spark.table("workspace.default.bronze_layer")

if watermark is not None:
    df = df.filter(F.col("ingestion_time") > F.expr(f"TIMESTAMP('{watermark}') - {WATERMARK_SAFETY_BUFFER}"))

df_final = df.select(
    F.col("transaction_id"),
    F.col("timestamp"),
    F.col("transaction_type"),
    F.col("status"),
    F.col("amount"),
    F.col("currency"),
    F.col("card_type"),
    F.col("entry_mode"),
    F.col("direction"),
    F.col("customer.customer_id").alias("customer_id"),
    F.col("customer.home_town").alias("home_town"),
    F.col("customer.lifestyle_archetype").alias("lifestyle_archetype"),
    F.col("customer.living_tier").alias("living_tier"),
    F.col("merchant.category").alias("category"),
    F.col("merchant.mcc").alias("mcc"),
    F.col("merchant.name").alias("merchant_name"),
    F.col("decline_reason"),
    F.col("source"),
    F.col("account_balance_before"),
    F.col("account_balance_after"),
    F.col("source_file_name"),
    F.col("ingestion_time"),
    F.col("ingested_at"),
)

df_final = df_final.dropDuplicates(["transaction_id"])

# ==========================================
# GREENBACKS POINTS CALCULATION
# (modeled on Nedbank's Greenbacks Rewards Programme guide, July 2026)
# ==========================================
# All reference values below are small, broadcast-joined dimension tables -- see
# setup_greenbacks_rules.sql for the seed data and the reasoning behind each one.
#
# Two real limitations of this model, stated plainly rather than papered over:
#   1. LEVEL is a proxy, not a real calculation. Nedbank recalculates level monthly
#      from 5 behavioral goals (salary deposits, digital transaction count, debit
#      order count, savings growth, loan repayment history). This generator has no
#      debit orders, loans, or savings accounts, so a real level can't be derived --
#      living_tier stands in for it via greenbacks_customer_level.
#   2. CARD CLASS is always treated as VISA_MASTERCARD_DEBIT. This generator only
#      ever produces "VISA"/"MASTERCARD" with no credit/debit distinction and no
#      Amex -- but since these simulated cards debit an actual account balance
#      rather than draw on a credit line, debit is the economically correct mapping,
#      and it's also the guide's lowest/most conservative earn tier.

config = {row["config_key"]: row["config_value"] for row in spark.table(CONFIG_TABLE).collect()}
GB_VALUE_RAND = config["gb_value_rand"]
BP_FUEL_REWARD_RAND_PER_LITRE = config["bp_fuel_reward_rand_per_litre"]
REFERENCE_FUEL_PRICE_PER_LITRE = config["reference_fuel_price_per_litre"]

level_earn_rates = spark.table(LEVEL_EARN_RATES_TABLE).filter(F.col("card_class") == "VISA_MASTERCARD_DEBIT")
customer_level = spark.table(CUSTOMER_LEVEL_TABLE)
category_rules = spark.table(CATEGORY_RULES_TABLE)
merchant_overrides = spark.table(MERCHANT_OVERRIDES_TABLE)

df_final = (
    df_final
    # Resolve this customer's (proxy) level, then their card-class/level earn rate
    .join(F.broadcast(customer_level), on="living_tier", how="left")
    .join(F.broadcast(level_earn_rates.select("level", "earn_rate")), on="level", how="left")
    .withColumn("earn_rate", F.coalesce(F.col("earn_rate"), F.lit(0.0)))
    # Resolve eligibility: merchant-level override takes precedence over the
    # category-level default (same override pattern used elsewhere in this pipeline
    # for merchant operating hours / online-payment capability).
    .join(F.broadcast(merchant_overrides), on="merchant_name", how="left")
    .join(
        F.broadcast(category_rules.withColumnRenamed("eligible", "category_eligible")),
        on="category", how="left"
    )
    .withColumn(
        "is_eligible",
        F.coalesce(F.col("eligible"), F.col("category_eligible"), F.lit(False))
    )
    .withColumn("bp_fuel_bonus", F.coalesce(F.col("bp_fuel_bonus"), F.lit(False)))
    .drop("eligible", "category_eligible")
    # Standard cashback: amount * earn_rate, only for eligible spend
    .withColumn(
        "standard_cashback_rand",
        F.when(F.col("is_eligible"), F.col("amount") * F.col("earn_rate")).otherwise(F.lit(0.0))
    )
    # bp fuel bonus: 25c per litre, litres = amount / reference fuel price -- only for
    # merchants flagged bp_fuel_bonus (BP Express in this dataset)
    .withColumn(
        "bp_bonus_rand",
        F.when(
            F.col("bp_fuel_bonus"),
            (F.col("amount") / F.lit(REFERENCE_FUEL_PRICE_PER_LITRE)) * F.lit(BP_FUEL_REWARD_RAND_PER_LITRE)
        ).otherwise(F.lit(0.0))
    )
    # Only APPROVED SPEND transactions ever earn points -- declines (nothing was
    # purchased) and deposits (not a purchase at all) always earn zero, regardless of
    # eligibility/rate lookups above.
    .withColumn(
        "points_earned",
        F.when(
            (F.col("transaction_type") == "SPEND") & (F.col("status") == "APPROVED"),
            F.floor((F.col("standard_cashback_rand") + F.col("bp_bonus_rand")) / F.lit(GB_VALUE_RAND))
        ).otherwise(F.lit(0)).cast("long")
    )
    .drop("level", "earn_rate", "is_eligible", "bp_fuel_bonus", "standard_cashback_rand", "bp_bonus_rand")
)

display(df_final)

df_final.createOrReplaceTempView("source_data")

spark.sql(f"""
    MERGE INTO {TARGET_TABLE} AS target
    USING source_data AS source
    ON target.transaction_id = source.transaction_id
    WHEN NOT MATCHED THEN
      INSERT *
""")
