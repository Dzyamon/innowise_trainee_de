# import task1_old
# import task1
import main
import pytest
import psycopg2
from dotenv import load_dotenv
import os


# test check if schema "task1" exists
# run with pytest -s (due to input)
@pytest.fixture(scope="module")
def db_connection():
    load_dotenv()
    conn = psycopg2.connect(host=os.environ.get("HOST"),
                            port=os.environ.get("PORT"),
                            dbname=os.environ.get('DATABASE'),
                            user=os.environ.get("USER_NAME"),
                            password=os.environ.get("USER_PASSWORD"))
    yield conn


def test_schema_exists(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'task1')")
    schema_exists = cursor.fetchone()[0]
    assert schema_exists, "Schema does not exist"
