# Location of this notebook to be placed in: workspace/IDE/ETL/DataIngestion/SourcetoLanding.py

from pyspark.sql import *
import json
from datetime import datetime
from pyspark.sql.functions import *

# Retrieve metadata from widgets
dbutils.widgets.text("metadata", "")
metadata = json.loads(dbutils.widgets.get("metadata"))

# Extract metadata fields
table_name = metadata["table_name"]
merge_strategy = metadata["merge_strategy"]
watermark_column = metadata["watermark_column"]
last_ingested_time = metadata["last_ingested_time"]
keyvault_name = metadata["keyvault_name"]
secret_name = metadata["secret_name"]
domain = metadata["domain"]
classification = metadata["classification"]

# Fetch connection string from Azure Key Vault
jdbc_url = dbutils.secrets.get(keyvault_name, secret_name)

# Example: Data extraction from SAP HANA based on merge strategy and watermark
if merge_strategy == "INC":
    query = f"SELECT * FROM {table_name} WHERE {watermark_column} > '{last_ingested_time}'"
else:
    query = f"SELECT * FROM {table_name}"

# Execute query and process data
df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({query}) as t").option("user", "username").option("password", "password").load()

# Save data to landing zone
now = datetime.now()
output_path = f"/mnt/landing/{domain}/{classification}/{now.strftime('%Y/%m/%d/%H%M')}/{table_name}.parquet"
df.write.parquet(output_path, mode="overwrite")

# Log the process details
total_row_count = df.count()
records_inserted = df.count()  # For incremental, calculate actual inserted count
records_updated = 0  # Update logic needed for incremental
records_deleted = 0  # Deletion logic if applicable

log_details = {
    "table_name": table_name,
    "database": metadata["database"],
    "domain": domain,
    "classification": classification,
    "Layer": "bronze",
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
