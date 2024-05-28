# Location of this notebook to be placed in: workspace/IDE/ETL/DataIngestion/SourcetoLanding.py
import json
from datetime import datetime

# Load metadata
metadata_path = dbutils.widgets.get("metadata_path")
with open(metadata_path, "r") as f:
    metadata_list = json.load(f)

for metadata in metadata_list:
    table_name = metadata["table_name"]
    load_type = metadata["load_type"]
    last_ingested_time = metadata["last_ingested_time"]
    domain = metadata["domain"]
    classification = metadata["classification"]
    
    # Load SAP credentials
    credentials_path = f"/dbfs/tmp/{table_name}_credentials.json"
    with open(credentials_path, "r") as f:
        credentials = json.load(f)
    
    sap_host = credentials["host"]
    sap_port = int(credentials["port"])
    sap_user = credentials["user"]
    sap_password = credentials["password"]
    
    # SAP HANA JDBC driver
    jdbc_url = f"jdbc:sap://{sap_host}:{sap_port}/"
    
    if load_type == "FULL":
        query = f"SELECT * FROM {table_name}"
    elif load_type == "INC":
        query = f"SELECT * FROM {table_name} WHERE last_modified > '{last_ingested_time}'"
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({query}) as {table_name}") \
        .option("user", sap_user) \
        .option("password", sap_password) \
        .load()
    
    # Perform any transformations
    transformed_df = df  # Apply your transformations here
    
    # Construct the output path
    now = datetime.now()
    output_path = f"/mnt/landing/{domain}/{classification}/{now.strftime('%Y/%m/%d/%H%M')}/{table_name}.parquet"
    
    # Save the transformed data to ADLS Gen2
    transformed_df.write.parquet(output_path, mode="overwrite")
