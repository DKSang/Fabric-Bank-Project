# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "477e91f0-29ee-45bb-b4ee-a9858b1de2b7",
# META       "default_lakehouse_name": "lh_finguard_gold",
# META       "default_lakehouse_workspace_id": "198ac056-e709-4dd6-bcfe-9ad6550ea321",
# META       "known_lakehouses": [
# META         {
# META           "id": "e31b1836-a85b-41f5-9285-7a1b50c90046"
# META         },
# META         {
# META           "id": "477e91f0-29ee-45bb-b4ee-a9858b1de2b7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_lakehouse_path = "abfss://198ac056-e709-4dd6-bcfe-9ad6550ea321@onelake.dfs.fabric.microsoft.com/e31b1836-a85b-41f5-9285-7a1b50c90046"
gold_lakehouse_path = "abfss://198ac056-e709-4dd6-bcfe-9ad6550ea321@onelake.dfs.fabric.microsoft.com/477e91f0-29ee-45bb-b4ee-a9858b1de2b7"

transactions = (
    spark.readStream.format("delta")
    .load(f"{silver_lakehouse_path}/Tables/dbo/transactions")
    .alias("transactions")
)
customers = (
    spark.read.format("delta")
    .load(f"{silver_lakehouse_path}/Tables/dbo/customers")
    .alias("customers")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

high_value_transactions_alert = (
    transactions
    .join(
        customers,
        F.col("transactions.customer_id") == F.col("customers.customer_id"),
        "left",
    )
    .filter(F.col("transactions.amount") > F.col("customers.transaction_limit"))
    .select(
        F.concat_ws("-", F.lit("ALERT"), F.col("transactions.transaction_id")).alias("alert_id"),
        F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
        F.current_timestamp().alias("alert_timestamp"),
        F.col("transactions.transaction_id").alias("transaction_id"),
        F.col("transactions.customer_id").alias("customer_id"),
        F.col("customers.email").alias("customer_email"),
        F.concat_ws(
            " ",
            F.col("customers.first_name"),
            F.col("customers.last_name"),
        ).alias("customer_name"),
        F.col("transactions.amount").alias("transaction_amount"),
        F.col("customers.transaction_limit").alias("transaction_limit"),
        F.col("transactions.currency").alias("currency"),
        F.col("transactions.merchant_name").alias("merchant_name"),
        F.col("transactions.merchant_category").alias("merchant_category"),
        F.col("transactions.transaction_type").alias("transaction_type"),
        F.col("transactions.payment_channel").alias("payment_channel"),
        F.col("transactions.city").alias("city"),
        F.col("transactions.country").alias("country"),
        F.col("transactions.is_international").alias("is_international"),
        F.col("transactions.transaction_timestamp").alias("transaction_timestamp"),
        F.col("transactions.status").alias("status"),
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = (
    high_value_transactions_alert.writeStream
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"{gold_lakehouse_path}/Files/finguard/gold/high_value_transactions_alert/checkpoint",
    )
    .trigger(processingTime="10 seconds")
    .toTable("dbo.high_value_transactions_alert")
)

query.awaitTermination()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
