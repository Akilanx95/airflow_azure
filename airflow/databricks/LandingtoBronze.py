# Location of this notebook to be placed in: workspace/IDE/ETL/DataIngestion/LandingtoBronze.py

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
source_system = metadata["source_system"]

# Load data from landing zone
now = datetime.now()
input_path = f"/mnt/landing/{domain}/{classification}/"
df = spark.read.parquet(input_path)

# Add inserttimestamp and source_system_name
df = df.withColumn("inserttimestamp", lit(now))
df = df.withColumn("source_system_name", lit(source_system))

# Define output path and save data to bronze layer
output_path = f"/mnt/bronze/{domain}/{classification}/{table_name}/{now.strftime('%Y/%m/%d/%H%M')}/"
df.write.format("delta").mode("overwrite").save(output_path)

# Log the process details
total_row_count = df.count()

log_details = {
    "table_name": table_name,
    "database": metadata["database"],
    "domain": domain,
    "classification": classification,
    "Layer": "bronze",
    "Total_row_count": total_row_count,
    "Records_inserted": total_row_count,
    "Records_updated": 0,
    "Records_deleted": 0,
    "timestamp": str(now)
}

log_path = f"/dbfs/tmp/{table_name}_log.json"
with open(log_path, "w") as log_file:
    json.dump(log_details, log_file)

# Output log details for Airflow
dbutils.notebook.exit(json.dumps(log_details))
