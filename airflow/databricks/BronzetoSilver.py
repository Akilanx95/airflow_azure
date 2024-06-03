# Location of this notebook to be placed in: workspace/IDE/ETL/DataIngestion/BronzetoSilver.py

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
merge_strategy = metadata["merge_strategy"]
merge_key = metadata["merge_key"]

# Load data from bronze table
bronze_table = f"bronze_{domain}_{classification}.{table_name}"
df_bronze = spark.read.format("delta").table(bronze_table)

# Define Silver table path and name
silver_table = f"silver_{domain}_{classification}.{table_name}"
silver_path = f"/mnt/silver/{domain}/{classification}/{table_name}/{now.strftime('%Y/%m/%d/%H%M')}/"

if merge_strategy == "INC":
    # Perform MERGE INTO operation
    df_silver = spark.read.format("delta").table(silver_table)
    
    # Define the merge condition
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_key.split(',')])
    
    # Perform the merge
    df_bronze.createOrReplaceTempView("source")
    df_silver.createOrReplaceTempView("target")
    spark.sql(f"""
        MERGE INTO {silver_table} AS target
        USING source
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
else:
    # Perform insert overwrite operation
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(silver_table)

# Save the data in the Silver layer location
df_bronze.write.format("delta").mode("overwrite").save(silver_path)

# Log the process details
total_row_count = df_bronze.count()
records_inserted = total_row_count  # For full load
records_updated = 0  # To be updated accordingly if incremental
records_deleted = 0  # To be updated accordingly if deletions occur

log_details = {
    "table_name": table_name,
    "database": metadata["database"],
    "domain": domain,
    "classification": classification,
    "Layer": "silver",
    "Total_row_count": total_row_count,
    "Records_inserted": records_inserted,
    "Records_updated": records_updated,
    "Records_deleted": records_deleted,
    "timestamp": str(now)
}

log_path = f"/dbfs/tmp/{table_name}_log.json"
with open(log_path, "w") as log_file:
    json.dump(log_details, log_file)

# Output log details for Airflow
dbutils.notebook.exit(json.dumps(log_details))
