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
# META           "id": "6bb65a11-d438-4fd0-b737-18b12947cead"
# META         },
# META         {
# META           "id": "5a3aca47-3afc-4d35-b7be-8a5f1c59ace4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vl=notebookutils.variableLibrary.getLibrary('Variables_1')
api_secret=vl.api_secret
api_key=vl.api_key
bootstrap_servers=vl.bootstrap_servers
topic=vl.topic

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

streaming_df=spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_servers) \
  .option("kafka.sasl.jaas.config", jaas_config) \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .load()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parsed_streaming_df=streaming_df.select(
col("key").cast("string"), col("value").cast("string"),
col("topic"), 
col("partition"), 
col("offset"),
col("timestamp"),
col("timestampType"),
F.current_timestamp().alias("ingestion_timestamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

streaming_query = (parsed_streaming_df.writeStream.format('delta') \
    .outputMode('append') \
    .option('checkpointLocation', 'abfss://198ac056-e709-4dd6-bcfe-9ad6550ea321@onelake.dfs.fabric.microsoft.com/6bb65a11-d438-4fd0-b737-18b12947cead/Files/finguard/bronze/source/transactions/checkpoint/') \
    .trigger(processingTime="10 seconds") \
    .toTable('dbo.transactions')
)
streaming_query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
