from pyspark.sql import functions as F
from pyspark.sql.window import Window
# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SILVER_TABLE = "workspace.default.silver_layer"
BRONZE_TABLE        = "workspace.default.bronze_layer"
 
CONFIG_TABLE             = "workspace.default.greenbacks_config"
LEVEL_EARN_RATES_TABLE   = "workspace.default.greenbacks_level_earn_rates"
CUSTOMER_LEVEL_TABLE     = "workspace.default.greenbacks_customer_level"
CATEGORY_RULES_TABLE     = "workspace.default.greenbacks_category_rules"
MERCHANT_OVERRIDES_TABLE = "workspace.default.greenbacks_merchant_overrides"
 
WATERMARK_SAFETY_BUFFER = "INTERVAL 1 HOURS"
 
SILVER_SCHEMA_VERSION = 2  # v1 = original schema, v2 = adds money conversion + cumulative points
 
# Master switch for the automatic reset-on-schema-mismatch
# Leave this True while you're actively testing schema changes. Once you're happy
# with the output and don't expect the schema to change again soon, flip this to
# False -- that disables the automatic reset (and the safety net it provides)
# without deleting any of the logic, so it's a one-line change to turn back on
# next time the schema changes again.
AUTO_RESET_ON_SCHEMA_MISMATCH = True

 
 
#  appends/updates -- there's no built-in way to "undo" a test run, and no way to
#  backfill a brand-new column onto rows that were written before it existed.
#  `reset_silver_layer(spark, confirm=True)` drops and recreates `silver_layer` with
#  a clean, empty schema. It's called both manually (ad hoc testing) and
#  automatically below whenever the schema version changes, so historical rows
#  that are missing new columns (e.g. points_value_rand, cumulative_points_earned)
#  never get left in the table alongside newly-shaped rows.
 
 
 
def reset_silver_layer(spark, confirm= False):
    if not confirm:
        raise ValueError(
            f"reset_silver_layer() permanently drops all existing data in "
            f"{TARGET_SILVER_TABLE}. This cannot be undone. Call "
            "reset_silver_layer(spark, confirm=True) if you're sure."
        )
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TARGET_SILVER_TABLE} (
            transaction_id                 STRING,
            timestamp                      TIMESTAMP,
            transaction_type               STRING,
            status                         STRING,
            amount                         DOUBLE,
            currency                       STRING,
            card_type                      STRING,
            entry_mode                     STRING,
            direction                      STRING,
            customer_id                    STRING,
            home_town                      STRING,
            lifestyle_archetype             STRING,
            living_tier                    STRING,
            category                       STRING,
            mcc                            STRING,
            merchant_name                  STRING,
            decline_reason                 STRING,
            source                         STRING,
            account_balance_before         DOUBLE,
            account_balance_after          DOUBLE,
            points_earned                  BIGINT,
            points_value_rand              DOUBLE,
            cumulative_points_earned       BIGINT,
            cumulative_points_value_rand   DOUBLE,
            ingestion_time                 TIMESTAMP,
            ingested_at                    TIMESTAMP
        ) USING DELTA
    """)
    print(f"{TARGET_SILVER_TABLE} has been dropped and recreated empty (schema v{SILVER_SCHEMA_VERSION}). "
          f"The next merge run will reprocess bronze from the start.")
 
 
 
# Forced reset on schema change (not just a manual testing utility anymore)
# Previously this was "testing only" and had to be called from a separate cell.
# That let a schema change (like adding points_value_rand / cumulative_* here) run
# MAGIC straight into `MERGE INTO` against an old-shaped table, silently leaving
# MAGIC historical rows with NULLs in the new columns forever, since MERGE only
# MAGIC touches rows it matches or inserts -- it never backfills untouched history.
# MAGIC We check the live table's columns against what this version of the pipeline
# MAGIC expects, and force a reset if they don't match, so this can never happen silently.
 
 
 
REQUIRED_SILVER_COLUMNS = {
    "transaction_id", "timestamp", "transaction_type", "status", "amount", "currency",
    "card_type", "entry_mode", "direction", "customer_id", "home_town",
    "lifestyle_archetype", "living_tier", "category", "mcc", "merchant_name",
    "decline_reason","account_balance_before", "account_balance_after",
    "points_earned", "points_value_rand", "cumulative_points_earned",
    "cumulative_points_value_rand", "ingestion_time", "ingested_at",
}
 
if not AUTO_RESET_ON_SCHEMA_MISMATCH:
    print(f"[INFO] AUTO_RESET_ON_SCHEMA_MISMATCH is False. Skipping automatic schema check/reset.")
else:
    _table_exists = spark.catalog.tableExists(TARGET_SILVER_TABLE)
    if _table_exists:
        _existing_columns = {f.name for f in spark.table(TARGET_SILVER_TABLE).schema.fields}
        if _existing_columns != REQUIRED_SILVER_COLUMNS:
            missing = REQUIRED_SILVER_COLUMNS - _existing_columns
            extra = _existing_columns - REQUIRED_SILVER_COLUMNS
            print(f"[WARN] {TARGET_SILVER_TABLE} schema does not match schema v{SILVER_SCHEMA_VERSION} "
                  f"(missing={missing or None}, unexpected={extra or None}). "
                  f"Resetting so historical rows missing these fields aren't carried forward.")
            reset_silver_layer(spark, confirm=True)
        else:
            print(f"[INFO] {TARGET_SILVER_TABLE} schema already matches v{SILVER_SCHEMA_VERSION}. No reset needed.")
    else:
        print(f"[INFO] {TARGET_SILVER_TABLE} does not exist yet. Will be created fresh below.")
  

# Create table if not exists
# Ensures the silver table exists before we try to read the watermark from it.
# This avoids the "TABLE_OR_VIEW_NOT_FOUND" error on first run. By the time we get
# here the schema check above has already guaranteed that if the table does exist,
# it's on the current schema -- so this is purely a first-run safety net now.
 
 
# Create the target table if it doesn't exist (idempotent - safe to run multiple times)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_SILVER_TABLE} (
        transaction_id                 STRING,
        timestamp                      TIMESTAMP,
        transaction_type               STRING,
        status                         STRING,
        amount                         DOUBLE,
        currency                       STRING,
        card_type                      STRING,
        entry_mode                     STRING,
        direction                      STRING,
        customer_id                    STRING,
        home_town                      STRING,
        lifestyle_archetype             STRING,
        living_tier                    STRING,
        category                       STRING,
        mcc                            STRING,
        merchant_name                  STRING,
        decline_reason                 STRING,
        account_balance_before         DOUBLE,
        account_balance_after          DOUBLE,
        points_earned                  BIGINT,
        points_value_rand              DOUBLE,
        cumulative_points_earned       BIGINT,
        cumulative_points_value_rand   DOUBLE,
        ingestion_time                 TIMESTAMP,
        ingested_at                    TIMESTAMP
    ) USING DELTA
""")
print(f"[INFO] Ensured {TARGET_SILVER_TABLE} exists (created or already existed)")
 
## Watermark
# Only pull bronze rows landed since the last successful merge, using the target
# table's own `MAX(ingestion_time)` as the watermark -- no separate checkpoint file
# to manage. Table is guaranteed to exist at this point, so no error handling needed.

watermark_row = spark.sql(f"""
    SELECT MAX(ingestion_time) - {WATERMARK_SAFETY_BUFFER} AS watermark
    FROM {TARGET_SILVER_TABLE}
""").collect()
watermark = watermark_row[0]["watermark"] if watermark_row and watermark_row[0]["watermark"] else None
 
if watermark:
    print(f"[INFO] Watermark from {TARGET_SILVER_TABLE}: {watermark} "
          f"(includes {WATERMARK_SAFETY_BUFFER} safety buffer)")
else:
    print(f"[INFO] {TARGET_SILVER_TABLE} exists but is empty. Will perform full load.")
 
 
df_bronze = spark.table(BRONZE_TABLE)
 
 
# Filter for incremental load: rows newer than watermark.
# This ensures we only process new data, not what's already in silver.
if watermark is not None:
    df_bronze = df_bronze.filter(
        F.col("ingestion_time") > F.lit(watermark)
    )
    print(f"[INFO] Filtering bronze rows with ingestion_time > {watermark}")
else:
    print(f"[INFO] No watermark found. Processing all bronze rows.")
 
 
# Filter out rows with a NULL transaction_id. This does NOT prevent a merge failure --
# ON target.transaction_id = source.transaction_id evaluates to NULL (not TRUE) when
# either side is null, so a null-keyed row simply never matches anything and would
# fall into WHEN NOT MATCHED on every single run, silently inserting a fresh duplicate
# null-keyed row each time this script executes. Filtering here avoids that unbounded
# duplicate accumulation. Logged so a real bronze data-quality issue doesn't vanish
# silently if this ever actually filters something out.
_pre_null_filter_count = df_bronze.count()
df_bronze = df_bronze.filter(F.col("transaction_id").isNotNull())
_dropped_null_id_count = _pre_null_filter_count - df_bronze.count()
if _dropped_null_id_count > 0:
    print(f"[WARN] Dropped {_dropped_null_id_count} bronze row(s) with a NULL transaction_id.")
 
 
# Unnest, clean, and normalise tracking columns

df_silver_source = df_bronze.select(
    F.col("transaction_id"),
    F.col("timestamp").cast("timestamp"),  # bronze stores this as an ISO string; cast to match silver's TIMESTAMP column
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
    F.col("ingestion_time"),
    F.col("ingested_at"),
)

# Keeps the most-recently-ingested row per `transaction_id`
# -- matters if bronze is
# ever fully refreshed and ends up with more than one copy of the same
# `transaction_id`. Tiebreaker on `ingested_at` keeps the result deterministic when
# `ingestion_time` matches exactly.
  
window_spec = Window.partitionBy("transaction_id").orderBy(
    F.col("ingestion_time").desc(),
    F.col("ingested_at").desc()
)
df_silver_clean = (
    df_silver_source
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
  
# Greenbacks points calculation
# Modeled on Nedbank's Greenbacks Rewards Programme guide (July 2026). Two real
# limitations of this model, stated plainly rather than papered over:
# 1. **Level** is a proxy, not a real calculation -- `living_tier` stands in for it
#  ia `greenbacks_customer_level`, since this generator has no debit orders,
#  loans, or savings accounts to derive a real monthly behavioral-goal level from.
# 2. **Card class** is always treated as `VISA_MASTERCARD_DEBIT`, since these
#   simulated cards debit an actual account balance rather than draw on a credit
#   line, and there's no Amex modeled.
# 
#  `points_earned` is a per-transaction figure. `points_value_rand` converts that
#  into an actual Rand amount at the configured GB_VALUE_RAND rate. 
  
config = {row["config_key"]: float(row["config_value"]) for row in spark.table(CONFIG_TABLE).collect()}
required_keys = ["gb_value_rand", "bp_fuel_reward_rand_per_litre", "reference_fuel_price_per_litre"]
for key in required_keys:
    if key not in config:
        raise ValueError(f"Missing required config key: {key}")
GB_VALUE_RAND = config["gb_value_rand"]
 
BP_FUEL_REWARD_RAND_PER_LITRE = config["bp_fuel_reward_rand_per_litre"]
REFERENCE_FUEL_PRICE_PER_LITRE = config["reference_fuel_price_per_litre"]
 
print(f"[INFO] Loaded config: GB_VALUE_RAND={GB_VALUE_RAND}, "
      f"BP_FUEL_REWARD_RAND_PER_LITRE={BP_FUEL_REWARD_RAND_PER_LITRE}, "
      f"REFERENCE_FUEL_PRICE_PER_LITRE={REFERENCE_FUEL_PRICE_PER_LITRE}")
 
 
level_earn_rates = spark.table(LEVEL_EARN_RATES_TABLE).filter(F.col("card_class") == "VISA_MASTERCARD_DEBIT")
customer_level = spark.table(CUSTOMER_LEVEL_TABLE)
category_rules = spark.table(CATEGORY_RULES_TABLE)
merchant_overrides = spark.table(MERCHANT_OVERRIDES_TABLE)
 
 
df_silver_clean = (
    df_silver_clean
    .join(F.broadcast(customer_level), on="living_tier", how="left")
    .join(F.broadcast(level_earn_rates.select("level", "earn_rate")), on="level", how="left")
    .withColumn("earn_rate", F.coalesce(F.col("earn_rate"), F.lit(0.0)))
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
    .withColumn(
        "standard_cashback_rand",
        F.when(F.col("is_eligible"), F.col("amount") * F.col("earn_rate")).otherwise(F.lit(0.0))
    )
    .withColumn(
        "bp_bonus_rand",
        F.when(
            F.col("bp_fuel_bonus"),
            (F.col("amount") / F.lit(REFERENCE_FUEL_PRICE_PER_LITRE)) * F.lit(BP_FUEL_REWARD_RAND_PER_LITRE)
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "points_earned",
        F.when(
            (F.col("transaction_type") == "SPEND") & (F.col("status") == "APPROVED"),
            F.floor((F.col("standard_cashback_rand") + F.col("bp_bonus_rand")) / F.lit(GB_VALUE_RAND))
        ).otherwise(F.lit(0)).cast("long")
    )
    # Money conversion: what those points are actually worth in Rand at the
    # configured redemption rate. Kept as its own persisted column rather than
    # something recomputed on the fly downstream, so BI / reporting tools don't
    # each need to know GB_VALUE_RAND themselves.
    .withColumn(
        "points_value_rand",
        F.round(F.col("points_earned") * F.lit(GB_VALUE_RAND), 2)
    )
    .drop("level", "earn_rate", "is_eligible", "bp_fuel_bonus", "standard_cashback_rand", "bp_bonus_rand")
)
 
 
df_silver_clean.createOrReplaceTempView("silver_source_view")
 
#  Merge into silver
#  `INSERT`/`VALUES` column lists are generated directly from `df_silver_clean`'s
#  schema (single source of truth) rather than hand-typed, so they can never drift
#  out of sync with each other or with the DataFrame itself. `cumulative_points_earned`
#  and `cumulative_points_value_rand` are deliberately NOT part of this merge --
#  they depend on every prior transaction for that customer, not just the row being
#  written, so they're computed in a dedicated pass right after (below), seeded with
#  0 here so the columns are never left NULL between the two steps.
  
silver_columns = df_silver_clean.columns
insert_column_list = ", ".join(silver_columns) + ", cumulative_points_earned, cumulative_points_value_rand"
insert_values_list = ", ".join(f"source.{c}" for c in silver_columns) + ", 0, 0.0"
 
print(f"[INFO] Merging {df_silver_clean.count()} rows into {TARGET_SILVER_TABLE}")
print(f"[INFO] Columns: {silver_columns}")
 
 
merge_result = spark.sql(f"""
    MERGE INTO {TARGET_SILVER_TABLE} AS target
    USING silver_source_view AS source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED AND target.status <> source.status THEN
      UPDATE SET
        target.status = source.status,
        target.account_balance_after = source.account_balance_after,
        target.decline_reason = source.decline_reason,
        target.points_earned = source.points_earned,
        target.points_value_rand = source.points_value_rand,
        target.ingestion_time = source.ingestion_time,
        target.ingested_at = source.ingested_at
    WHEN NOT MATCHED THEN
      INSERT ({insert_column_list})
      VALUES ({insert_values_list})
""")
 
 
# merge_result carries the MERGE's own operation metrics (num_inserted_rows,
# num_updated_rows, num_affected_rows, ...) -- reporting these directly is both
# accurate and free.
stats = merge_result.collect()[0].asDict()
print(
    f"Silver layer merge completed. "
    f"Inserted: {stats.get('num_inserted_rows', 'n/a')}, "
    f"Updated: {stats.get('num_updated_rows', 'n/a')}, "
    f"Total affected: {stats.get('num_affected_rows', 'n/a')}"
)
 

# Recompute cumulative points per customer
# This is the piece that was missing entirely before: `points_earned` was only ever
# a per-transaction figure, so there was nowhere a customer's running total lived --
# it looked like points "didn't accumulate" because nothing was summing them.
# A per-customer running window, ordered by transaction time, is recomputed over the
# *whole* target table (not just this run's incremental rows) every run. That's
# intentionally not incremental: a status flip on an old transaction (e.g. a decline
# reversed to approved days later) changes that row's points_earned, which must
# ripple through the running total of every later transaction for that customer --
# MAGIC an incremental-only update would miss that.
 

cumulative_window = (
    Window.partitionBy("customer_id")
    .orderBy(F.col("timestamp").asc(), F.col("transaction_id").asc())
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
 
df_with_cumulative = (
    spark.table(TARGET_SILVER_TABLE)
    .withColumn("cumulative_points_earned", F.sum("points_earned").over(cumulative_window))
    .withColumn(
        "cumulative_points_value_rand",
        F.round(F.col("cumulative_points_earned") * F.lit(GB_VALUE_RAND), 2)
    )
    .select("transaction_id", "cumulative_points_earned", "cumulative_points_value_rand")
)
 
df_with_cumulative.createOrReplaceTempView("silver_cumulative_view")
 
cumulative_result = spark.sql(f"""
    MERGE INTO {TARGET_SILVER_TABLE} AS target
    USING silver_cumulative_view AS source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED THEN
      UPDATE SET
        target.cumulative_points_earned = source.cumulative_points_earned,
        target.cumulative_points_value_rand = source.cumulative_points_value_rand
""")
 
cumulative_stats = cumulative_result.collect()[0].asDict()
print(
    f"Cumulative points recompute completed. "
    f"Rows updated: {cumulative_stats.get('num_updated_rows', 'n/a')}"
)

# Validating the cummulative totals

validation_df = (
    spark.table(TARGET_SILVER_TABLE)
    .groupBy("customer_id")
    .agg(
        F.sum("points_earned").alias("expected_total_points"),
        F.max("cumulative_points_earned").alias("recorded_total_points"),
    )
    .filter(F.col("expected_total_points") != F.col("recorded_total_points"))
)
 
_mismatch_count = validation_df.count()
if _mismatch_count > 0:
    print(f"[ERROR] Cumulative points mismatch detected for {_mismatch_count} customer(s):")
    validation_df.show(truncate=False)
    raise AssertionError(
        f"cumulative_points_earned does not match an independently computed "
        f"SUM(points_earned) for {_mismatch_count} customer(s) in {TARGET_SILVER_TABLE}. "
        f"Aborting rather than leaving inconsistent running totals in the table."
    )
else:
    print(f"[INFO] Cumulative points validated: running totals match independently "
          f"computed lifetime sums for every customer in {TARGET_SILVER_TABLE}.")
 
