<<<<<<< HEAD
import task1_old
=======
import task1
>>>>>>> dev
import pytest
import psycopg2


# test check if schema "task1" exists
<<<<<<< HEAD
# run with pytest -s (due to input)
@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(host=task1_old.host,
                            port=task1_old.port,
                            dbname=task1_old.database,
                            user=task1_old.user_name,
                            password=task1_old.user_password)
=======
@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(host=task1.host,
                            port=task1.port,
                            dbname=task1.database,
                            user=task1.user_name,
                            password=task1.user_password)
>>>>>>> dev
    yield conn


def test_schema_exists(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'task1')")
    schema_exists = cursor.fetchone()[0]
    assert schema_exists, "Schema does not exist"
