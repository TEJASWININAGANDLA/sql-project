import logging
import os
import snowflake.connector
import pandas as pd
import numpy as np

# Configuration
SNOWFLAKE_CONFIG = {
    'user': 'Tejaswini',
    'password': 'Tejaswini01808',
    'account': 'ormrinl-pk30949',
    'warehouse': 'COMPUTE_WH',
    'database': 'P01_EMP',
    'schema_raw': 'EMP_RAW',
}

class DataIngestion:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        try:
            self.conn = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema_raw']
            )
            logging.info("Connected to Snowflake successfully.")
        except Exception as e:
            logging.error(f"Error connecting to Snowflake: {e}")
            exit(1)

    def create_table(self, table_name, columns):
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})")
            logging.info(f"Table {table_name} created successfully.")
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {e}")
            exit(1)

    def ingest_data(self, file_path, table_name):
        try:
            if os.path.exists(file_path):
                # Read txt file without header
                data = pd.read_csv(file_path, delimiter='|', header=None, dtype=str)

                # Replace 'NULL' strings with np.nan
                data.replace('NULL', np.nan, inplace=True)

                # Debug: Print the DataFrame to ensure it's read correctly
                print("DataFrame:")
                print(data)

                # Define columns for table creation
                columns = [f'"COLUMN{i}" VARCHAR' for i in range(1, len(data.columns) + 1)]

                # Debug: Print column definitions
                print("Columns:", columns)

                self.create_table(table_name, columns)

                cursor = self.conn.cursor()

                for _, row in data.iterrows():
                    print(f"Inserting row: {row}")  # Debug line to print each row
                    try:
                        values = [None if pd.isna(val) else val for val in row]
                        cursor.execute(f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in values])})", values)
                    except Exception as e:
                        logging.error(f"Error inserting row {row}: {e}")

                self.conn.commit()
                logging.info(f"Ingested {len(data)} rows into table {table_name}.")
            else:
                logging.error(f"File {file_path} does not exist.")
        except Exception as e:
            logging.error(f"Error ingesting data from file {file_path} to table {table_name}: {e}")
            exit(1)

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Snowflake connection closed.")

if __name__ == "__main__":
    logging.basicConfig(filename='ingestion.log', level=logging.INFO)

    files_to_tables = {
        'dependent.txt': 'DEPENDENT',
        'department.txt': 'DEPARTMENT',
        'employee.txt': 'EMPLOYEE',
        'project.txt': 'PROJECT',
        'dept_locations.txt': 'DEPT_LOCATIONS',
        'works_on.txt': 'WORKS_ON',
    }

    ingestion = DataIngestion(SNOWFLAKE_CONFIG)
    ingestion.connect()

    for file_name, table_name in files_to_tables.items():
        file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'inbound_data', file_name))
        ingestion.ingest_data(file_path, table_name)

    ingestion.close()
