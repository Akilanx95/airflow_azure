# Use the official Airflow image
FROM apache/airflow:2.4.1

# Install any additional packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and plugins
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/

# Set the entrypoint to Airflow command
ENTRYPOINT ["airflow", "standalone"]
