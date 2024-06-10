import snowflake.connector
import os
import logging
import sys

# Logging configuration
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'emp_data_ingestion.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SnowflakeConnector:
    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None

    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            logging.info("Successfully connected to Snowflake.")
        except snowflake.connector.errors.Error as e:
            logging.error(f"Failed to connect to Snowflake: {e}")
            sys.exit(1)

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info("Closed the Snowflake connection.")

    def get_cursor(self):
        if self.connection:
            return self.connection.cursor()
        else:
            logging.error("Connection not established.")
            return None

class DataIngestion:
    def __init__(self, connector, input_dir):
        self.connector = connector
        self.input_dir = input_dir

    def ingest_data(self, table_name, file_name):
        file_path = os.path.join(self.input_dir, file_name)
        logging.debug(f"Processing file: {file_path}")

        if not os.path.exists(file_path):
            logging.error(f"File {file_name} not found in {self.input_dir}.")
            return

        try:
            self.connector.connect()
            cursor = self.connector.get_cursor()

            with open(file_path, 'r') as file:
                headers = file.readline().strip().split('|')
                columns = ', '.join(headers)

                for line in file:
                    values = line.strip().split('|')
                    if values:
                        values = [f"'{val.replace('\'', '\'\'')}'" if val else 'NULL' for val in values]
                        values_str = ', '.join(values)
                        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})"
                        logging.debug(f"Executing query: {query}")
                        cursor.execute(query)

            self.connector.connection.commit()
            logging.info(f"Data ingested into {table_name} from {file_name}.")
        except Exception as e:
            logging.error(f"Error during data ingestion for {table_name}: {e}")
        finally:
            self.connector.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python emp_data_ingestion.py <input_directory>")
        sys.exit(1)

    input_dir = sys.argv[1]
    if not os.path.isdir(input_dir):
        print("Error: Input directory not found.")
        sys.exit(1)

    # Initialize SnowflakeConnector
    connector = SnowflakeConnector(
        user='TEJASWINI',
        password='Tejaswini01808',
        account='ormrinl-pk30949',
        warehouse='COMPUTE_WH',
        database='P01_EMP',
        schema='EMP_RAW'
    )

    # Initialize DataIngestion
    ingestion = DataIngestion(connector, input_dir)

    # Ingest data for each table
    table_files = {
        'EMPLOYEE': 'employee.txt',
        'DEPARTMENT': 'department.txt',
        'DEPT_LOCATIONS': 'dept_locations.txt',
        'PROJECT': 'project.txt',
        'WORKS_ON': 'works_on.txt',
        'DEPENDENT': 'dependent.txt'
    }

    for table, file in table_files.items():
        logging.info(f"Ingesting data into {table} from {file}")
        ingestion.ingest_data(table, file)

    print("Data ingestion completed.")
    logging.info("Data ingestion completed.")

if __name__ == "__main__":
    main()
