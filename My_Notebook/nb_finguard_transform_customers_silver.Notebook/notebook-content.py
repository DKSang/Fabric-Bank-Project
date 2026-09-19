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

bronze_table_url='abfss://198ac056-e709-4dd6-bcfe-9ad6550ea321@onelake.dfs.fabric.microsoft.com/5a3aca47-3afc-4d35-b7be-8a5f1c59ace4/Tables/dbo/customers'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df=spark.read.option('format','delta').load(bronze_table_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = (
    bronze_df
    .select("*")
    .withColumn("account_open_date", col("account_open_date").cast("date"))
    .withColumn("silver_ingestion_time_stamp", F.current_timestamp())
)

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dbo.customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
