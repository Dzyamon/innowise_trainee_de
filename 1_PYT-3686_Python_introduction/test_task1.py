import task1
import pytest
import psycopg2


# test check if schema "task1" exists
@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(host=task1.host,
                            port=task1.port,
                            dbname=task1.database,
                            user=task1.user_name,
                            password=task1.user_password)
    yield conn


def test_schema_exists(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'task1')")
    schema_exists = cursor.fetchone()[0]
    assert schema_exists, "Schema does not exist"
