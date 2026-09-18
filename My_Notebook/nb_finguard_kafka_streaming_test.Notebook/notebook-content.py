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
# META           "id": "f8413223-a162-4193-b45e-4f9b2d318c32"
# META         },
# META         {
# META           "id": "5a3aca47-3afc-4d35-b7be-8a5f1c59ace4"
# META         },
# META         {
# META           "id": "6bb65a11-d438-4fd0-b737-18b12947cead"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# # Welcome to your new notebook
# # Type here in the cell editor to add code!
# api_secret='cfltdPtUlVclnD+i9A64l/PvElxOOmr7nUAqsOPuYymcbrjU1iKZfZEm1YklxpbQ'
# api_key='HF5INLC2FJNJEUCL'
# bootstrap_servers='pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092'
# topic='credit_card_transactions'

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

sample_batch=spark.read.format("kafka") \
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

sample_batch.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sample_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
parsed_batch=sample_batch.select(
col("key").cast("string"), col("value").cast("string"),
col("topic"), 
col("partition"), 
col("offset"),
col("timestamp"),
col("timestampType")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(parsed_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parsed_batch.write.saveAsTable("lh_finguard_bronze.dbo.transactions_batch_test")

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

from pyspark.sql.functions import col
parsed_streaming_df=streaming_df.select(
col("key").cast("string"), col("value").cast("string"),
col("topic"), 
col("partition"), 
col("offset"),
col("timestamp"),
col("timestampType")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

streaming_query = (parsed_streaming_df.writeStream.format('delta') \
    .outputMode('append') \
    .option('checkpointLocation', 'abfss://198ac056-e709-4dd6-bcfe-9ad6550ea321@onelake.dfs.fabric.microsoft.com/6bb65a11-d438-4fd0-b737-18b12947cead/Files/finguard/source/transactions/checkpoint') \
    .trigger(availableNow=True) \
    .toTable('dbo.transactions_streaming_test')
)
print("Query ID :",streaming_query.id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dbo.transactions_streaming_test

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
