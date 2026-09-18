# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8413223-a162-4193-b45e-4f9b2d318c32",
# META       "default_lakehouse_name": "lh_banking_raw",
# META       "default_lakehouse_workspace_id": "198ac056-e709-4dd6-bcfe-9ad6550ea321",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8413223-a162-4193-b45e-4f9b2d318c32"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
raw_path = "/lakehouse/lh_banking_raw/Files/banking_datasets/"
accounts_df=spark.read.option("header", "true").option("inferSchema", "true").csv(f"{raw_path}accounts.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

