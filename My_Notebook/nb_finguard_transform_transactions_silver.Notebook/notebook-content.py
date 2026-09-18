# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5a3aca47-3afc-4d35-b7be-8a5f1c59ace4",
# META       "default_lakehouse_name": "lh_finguard_bronze",
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df=spark.readStream.table('dbo.transactions')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformed_df=bronze_df.select(
    F.from_json(col('value'),schema).alias()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
