import logging
import os
import snowflake.connector

# Configuration
SNOWFLAKE_CONFIG = {
    'user': 'Tejaswini',
    'password': 'Tejaswini01808',
    'account': 'ormrinl-pk30949',
    'warehouse': 'COMPUTE_WH',
    'database': 'P01_EMP',
    'schema_raw': 'EMP_RAW',
    'schema_proc': 'EMP_PROC'
}

# Setup logging
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'emp_data_processing.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Ensure extracts directory exists
extracts_dir = '../extracts'
if not os.path.exists(extracts_dir):
    os.makedirs(extracts_dir)

class DataProcessing:
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

    def execute_query(self, query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            logging.info("Executed query: %s", query)
            return cursor
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return None

    def generate_report(self, query, file_name):
        try:
            cursor = self.execute_query(query)
            if cursor:
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]

                file_path = os.path.join(extracts_dir, file_name)
                with open(file_path, 'w') as file:
                    file.write('|'.join(column_names) + '\n')
                    for row in results:
                        file.write('|'.join(map(str, row)) + '\n')

                logging.info(f"Report generated: {file_name}")
        except Exception as e:
            logging.error(f"Error generating report {file_name}: {e}")

    def process_data(self):
        try:
            # Create EMP_PROC schema
            self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.config['schema_proc']}")
            self.conn.cursor().execute(f"USE SCHEMA {self.config['schema_proc']}")

            # Report 1: Employees with salaries greater than their managers
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_sal_greater_mngr AS
                SELECT e1.essn, e1.salary, e1.super_ssn, e2.salary AS mgr_salary
                FROM EMP_RAW.EMPLOYEE e1
                JOIN EMP_RAW.EMPLOYEE e2 ON e1.super_ssn = e2.essn
                WHERE e1.salary > e2.salary;
            """)
            self.generate_report("SELECT * FROM emp_sal_greater_mngr", "emp_sal_greater_mngr.txt")

            # Report 2: Employees working on projects of other departments
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_project_dept AS
                SELECT e.essn, p.pname, d1.dname AS emp_dept_name, d2.dname AS proj_dept_name
                FROM EMP_RAW.EMPLOYEE e
                JOIN EMP_RAW.DEPARTMENT d1 ON e.dno = d1.dnumber
                JOIN EMP_RAW.WORKS_ON w ON e.essn = w.essn
                JOIN EMP_RAW.PROJECT p ON w.pno = p.pnumber
                JOIN EMP_RAW.DEPARTMENT d2 ON p.dnum = d2.dnumber
                WHERE e.dno != p.dnum;
            """)
            self.generate_report("SELECT * FROM emp_project_dept", "emp_project_dept.txt")

            # Report 3: Department with the least number of employees
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_dept_least AS
                SELECT d.dname, d.dnumber, COUNT(e.essn) AS no_of_emp
                FROM EMP_RAW.DEPARTMENT d
                LEFT JOIN EMP_RAW.EMPLOYEE e ON d.dnumber = e.dno
                GROUP BY d.dname, d.dnumber
                ORDER BY no_of_emp ASC
                LIMIT 1;
            """)
            self.generate_report("SELECT * FROM emp_dept_least", "emp_dept_least.txt")

            # Report 4: Total hours spent on a project by employees with dependents
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_tot_hrs_spent AS
                SELECT e.essn, d.dependent_name, w.pno, SUM(w.hours) AS total_hours_spent
                FROM EMP_RAW.EMPLOYEE e
                JOIN EMP_RAW.DEPENDENT d ON e.essn = d.essn
                JOIN EMP_RAW.WORKS_ON w ON e.essn = w.essn
                GROUP BY e.essn, d.dependent_name, w.pno;
            """)
            self.generate_report("SELECT * FROM emp_tot_hrs_spent", "emp_tot_hrs_spent.txt")

            # Report 5: Full details of employees, their projects, and dependents
            self.execute_query("""
                CREATE OR REPLACE TABLE emp_full_details AS
                SELECT 
                    e.*, d.dname, p.pname, p.pnumber, p.plocation, w.hours AS total_hours,
                    de.dependent_name, de.sex AS dependent_sex, de.relationship AS dependent_relation
                FROM P01_EMP.EMP_RAW.EMPLOYEE e
                JOIN P01_EMP.EMP_RAW.DEPARTMENT d ON e.dno = d.dnumber
                JOIN P01_EMP.EMP_RAW.WORKS_ON w ON e.essn = w.essn
                JOIN P01_EMP.EMP_RAW.PROJECT p ON w.pno = p.pnumber
                LEFT JOIN P01_EMP.EMP_RAW.DEPENDENT de ON e.essn = de.essn;
            """)
            self.generate_report("SELECT * FROM emp_full_details", "emp_full_details.txt")

        except Exception as e:
            logging.error(f"Error processing data: {e}")

if __name__ == "__main__":
    processor = DataProcessing(SNOWFLAKE_CONFIG)
    processor.connect()
    processor.process_data()
    processor.conn.close()
