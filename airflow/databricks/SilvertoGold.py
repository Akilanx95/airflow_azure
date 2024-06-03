# Location of this notebook to be placed in: workspace/IDE/ETL/DataIngestion/SilvertoGold.py

from pyspark.sql import *
import json
from datetime import datetime

# Retrieve metadata from widgets
dbutils.widgets.text("metadata", "")
metadata = json.loads(dbutils.widgets.get("metadata"))

# Extract metadata fields
table_name = metadata["table_name"]
domain = metadata["domain"]
classification = metadata["classification"]

# Load data from silver table
silver_table = f"silver_{domain}_{classification}.{table_name}"
df_silver = spark.read.format("delta").table(silver_table)

# Define Gold table path and name
gold_table = f"{domain}_{classification}.{table_name}"
gold_path = f"/mnt/gold/{domain}/{classification}/{table_name}/{datetime.now().strftime('%Y/%m/%d/%H%M')}/"

# Perform insert overwrite operation
df_silver.write.format("delta").mode("overwrite").saveAsTable(gold_table)

# Save the data in the Gold layer location
df_silver.write.format("delta").mode("overwrite").save(gold_path)

# Write the Gold table to PostgreSQL
df_silver.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://<your_postgresql_host>:<your_postgresql_port>/<your_database>") \
    .option("dbtable", gold_table) \
    .option("user", "<your_postgresql_user>") \
    .option("password", "<your_postgresql_password>") \
    .save()

# Log the process details
total_row_count = df_silver.count()

log_details = {
    "table_name": table_name,
    "database": metadata["database"],
    "domain": domain,
    "classification": classification,
    "Layer": "gold",
    "Total_row_count": total_row_count,
    "Records_inserted": total_row_count,
    "Records_updated": 0,
    "Records_deleted": 0,
    "timestamp": str(datetime.now())
}

log_path = f"/dbfs/tmp/{table_name}_log.json"
with open(log_path, "w") as log_file:
    json.dump(log_details, log_file)

# Output log details for Airflow
dbutils.notebook.exit(json.dumps(log_details))
