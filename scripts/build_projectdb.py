import psycopg2 as psql
from pprint import pprint
import os
import re

# Read password from secrets file
secret_path = os.path.join("secrets", ".psql.pass")
with open(secret_path, "r") as file:
    password = file.read().strip()

# Build connection string
conn_string = f"host=hadoop-04.uni.innopolis.ru port=5432 user=team15 dbname=team15_projectdb password={password}"

# Connect to the remote DBMS
with psql.connect(conn_string) as conn:
    cur = conn.cursor()

    # === Create tables ===
    with open(os.path.join("sql", "create_tables.sql")) as file:
        create_sql = file.read()
        cur.execute(create_sql)
    conn.commit()

    # === Extract full COPY commands from import_data.sql ===
    with open(os.path.join("sql", "import_data.sql")) as file:
        sql_text = file.read()

    # Найти все блоки COPY ... ;
    commands = re.findall(r"COPY\s.*?FROM\sSTDIN.*?;", sql_text, re.DOTALL | re.IGNORECASE)

    if len(commands) != 3:
        raise ValueError(f"Expected 3 COPY commands in import_data.sql, found {len(commands)}")

    # === Import data ===
    with open(os.path.join("data", "airports.csv"), "r") as airport_file:
        cur.copy_expert(commands[0], airport_file)

    with open(os.path.join("data", "cancellation_reasons.csv"), "r") as cancel_file:
        cur.copy_expert(commands[1], cancel_file)

    with open(os.path.join("data", "combine_files_upd.csv"), "r") as flight_file:
        cur.copy_expert(commands[2], flight_file)

    conn.commit()

    # === Run test queries ===
    with open(os.path.join("sql", "test_database.sql")) as file:
        test_commands = [line.strip() for line in file if line.strip() and not line.strip().startswith("--")]
        for command in test_commands:
            cur.execute(command)
            try:
                result = cur.fetchall()
                pprint(result)
            except psql.ProgrammingError:
                # In case it's not a SELECT
                print("Query executed:", command)