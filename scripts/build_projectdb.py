"""
This script initializes a Spark session with Hive support for team15's Big Data project.
It connects to the Hive metastore and sets up the Spark environment for running ML tasks.
"""

import re
from pprint import pprint
import os
import psycopg2 as psql

secret_path = os.path.join("secrets", ".psql.pass")
with open(secret_path, "r", encoding="utf-8") as file:
    password = file.read().strip()

CONN = (
    f"host=hadoop-04.uni.innopolis.ru "
    f"port=5432 "
    f"user=team15 "
    f"dbname=team15_projectdb "
    f"password={password}"
)

with psql.connect(CONN) as conn:
    cur = conn.cursor()

    with open(os.path.join("sql", "create_tables.sql"), "r", encoding="utf-8") as file:
        create_sql = file.read()
        cur.execute(create_sql)
    conn.commit()

    with open(os.path.join("sql", "import_data.sql"), "r", encoding="utf-8") as file:
        sql_text = file.read()

    commands = re.findall(r"COPY\s.*?FROM\sSTDIN.*?;",
                          sql_text, re.DOTALL | re.IGNORECASE)

    if len(commands) != 3:
        raise ValueError(
            f"Expected 3 COPY commands in import_data.sql, found {len(commands)}")

    with open(os.path.join("data", "airports.csv"), "r", encoding="utf-8") as airport_file:
        cur.copy_expert(commands[0], airport_file)

    with open(os.path.join("data", "cancellation_reasons.csv"),
              "r", encoding="utf-8") as cancel_file:
        cur.copy_expert(commands[1], cancel_file)

    with open(os.path.join("data", "combine_files_upd.csv"), "r", encoding="utf-8") as flight_file:
        cur.copy_expert(commands[2], flight_file)

    conn.commit()

    with open(os.path.join("sql", "test_database.sql"), "r", encoding="utf-8") as file:
        test_commands = [line.strip() for line in file if line.strip()
                         and not line.strip().startswith("--")]
        for command in test_commands:
            cur.execute(command)
            try:
                result = cur.fetchall()
                pprint(result)
            except psql.ProgrammingError:
                print("Query executed:", command)
