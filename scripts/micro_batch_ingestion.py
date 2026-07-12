from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

event_schema = StructType([
    StructField("transaction_id",       StringType()),
    StructField("timestamp",            StringType()),
    StructField("amount",               DoubleType()),
    StructField("currency",             StringType()),
    StructField("transaction_type",     StringType()),   # SPEND | DECLINE | DEPOSIT
    StructField("status",               StringType()),   # APPROVED | DECLINED
    StructField("direction",            StringType()),   # DEBIT | CREDIT
    StructField("card_type",            StringType()),   # SPEND/DECLINE only
    StructField("entry_mode",           StringType()),   # SPEND/DECLINE only
    StructField("decline_reason",       StringType()),   # DECLINE only
    StructField("source",               StringType()),   # DEPOSIT only
    StructField("customer", StructType([
        StructField("customer_id",         StringType()),
        StructField("lifestyle_archetype", StringType()),
        StructField("living_tier",         StringType()),
        StructField("home_town",           StringType()),
    ])),
    StructField("merchant", StructType([
        StructField("name",     StringType()),
        StructField("mcc",      StringType()),
        StructField("category", StringType()),
    ])),
    StructField("account_balance_before", DoubleType()),
    StructField("account_balance_after",  DoubleType()),
])

# 2. READ STREAM
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/rewards_catalog/loyalty/landing/_schemas/bronze")
    .schema(event_schema)
    .load("/Volumes/rewards_catalog/loyalty/landing/card_spend_landing")
    .select(
        "*",
        "_metadata.file_name",
        "_metadata.file_path",
        "_metadata.file_modification_time",
        F.current_timestamp().alias("ingested_at"),  # evaluated once per row
    )
)

# 3. WRITE STREAM
query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/rewards_catalog/loyalty/landing/_checkpoints/bronze")
    .trigger(availableNow=True)
    .table("rewards_catalog.loyalty.bronze_card_spend"))  # .table() starts the stream automatically
