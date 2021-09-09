import os
import psycopg2

# Pulls in data from a dump generated from postgres.
# The dump will likely need to be regenerated from time to time.
# An example of doing that is:
#
# pg_dump -U brxycailpjqogm -h ec2-3-222-127-167.compute-1.amazonaws.com -p 5432 -d db1e9cbi07g30k -s -f ./dags/tests/fixtures/db_schema


def db_connect():
    return psycopg2.connect(
        host="localhost",
        database="shovel_test_database",
        password=os.environ.get("POSTGRES_PASSWORD"),
        user=os.environ.get("POSTGRES_USER"),
    )


def create_test_database():
    pg_connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        password=os.environ.get("POSTGRES_PASSWORD"),
        user=os.environ.get("POSTGRES_USER"),
    )
    pg_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = pg_connection.cursor()
    cursor.execute("DROP DATABASE IF EXISTS shovel_test_database;")
    cursor.execute("DROP USER IF EXISTS brxycailpjqogm;")
    cursor.execute("CREATE DATABASE shovel_test_database;")
    cursor.execute("CREATE USER brxycailpjqogm;")
    cursor.execute(
        "GRANT ALL PRIVILEGES ON DATABASE shovel_test_database TO brxycailpjqogm;"
    )
    cursor.close()
    pg_connection.close()

    pg_connection = db_connect()
    pg_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = pg_connection.cursor()
    create_schema = open(
        f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/db_schema", "r"
    )
    cursor.execute(create_schema.read())
    cursor.close()
    pg_connection.close()
