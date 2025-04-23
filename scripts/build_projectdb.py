import psycopg2 as psql
from pprint import pprint
import os


def read_password():
    path = os.path.join("secrets", ".psql.pass")
    with open(path, "r") as f:
        return f.read().strip()

def get_connection(password):
    return psql.connect(
        host="10.100.30.57",
        port=5432,
        user="team15",
        dbname="team15_projectdb",
        password=password
    )        

def execute_sql_file(cursor, filepath):
    with open(filepath, "r") as file:
        cursor.execute(file.read())

def copy_data(cursor, filepath, copy_command):
    with open(filepath, "r") as f:
        cursor.copy_expert(copy_command, f)

def main():
    password = read_password()

    with get_connection(password) as conn:
        cur = conn.cursor()

        # Step 1: Create table
        print("Creating table...")
        execute_sql_file(cur, os.path.join("sql", "create_tables.sql"))
        conn.commit()

        # Step 2: Import data
        print("Importing data...")
        with open(os.path.join("sql", "import_data.sql"), "r") as file:
            copy_sql = file.read()
        copy_data(cur, os.path.join("data", "preprocessed_combine_files.csv"), copy_sql)
        conn.commit()

        # Step 3: Run test queries
        print("Running test queries...")
        with open(os.path.join("sql", "test_database.sql"), "r") as file:
            for query in file.read().split(";"):
                query = query.strip()
                if query:
                    cur.execute(query)
                    if cur.description:
                        pprint(cur.fetchall())

# if __name__ == "__main__":
#     main()

def test_connection():
    password = read_password()
    try:
        with get_connection(password) as conn:
            print("Connection successful!")
    except Exception as e:
        print("Connection failed:", e)

if __name__ == "__main__":
    test_connection()
