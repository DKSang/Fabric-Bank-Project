# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e31b1836-a85b-41f5-9285-7a1b50c90046",
# META       "default_lakehouse_name": "lh_finguard_silver",
# META       "default_lakehouse_workspace_id": "198ac056-e709-4dd6-bcfe-9ad6550ea321",
# META       "known_lakehouses": [
# META         {
# META           "id": "5a3aca47-3afc-4d35-b7be-8a5f1c59ace4"
# META         },
# META         {
# META           "id": "6bb65a11-d438-4fd0-b737-18b12947cead"
# META         },
# META         {
# META           "id": "e31b1836-a85b-41f5-9285-7a1b50c90046"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    DecimalType, 
    BooleanType, 
    TimestampType
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_table_url='abfss://198ac056-e709-4dd6-bcfe-9ad6550ea321@onelake.dfs.fabric.microsoft.com/5a3aca47-3afc-4d35-b7be-8a5f1c59ace4/Tables/dbo/transactions'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df=spark.readStream.option('format','delta').load(bronze_table_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

valid_filter = (
    col("transaction_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("card_number").isNotNull() &
    col("merchant_id").isNotNull() &
    (col("amount") > 0)
)

silver_clean_df = bronze_df.filter(valid_filter).col()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

streaming_query=(
    silver_clean_df.writeStream.outputMode("append")\
    .option('checkpointLocation','abfss://198ac056-e709-4dd6-bcfe-9ad6550ea321@onelake.dfs.fabric.microsoft.com/6bb65a11-d438-4fd0-b737-18b12947cead/Files/finguard/silver/source/transactions/checkpoint/')
    .trigger(processingTime="10 seconds")\
    .toTable("dbo.transactions")
)
streaming_query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
