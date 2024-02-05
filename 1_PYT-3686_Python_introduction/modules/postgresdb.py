import pandas as pd
import psycopg2
import logging

class Postgres:
    """
    The Postgres class is a utility class that contains various
    methods for interacting with a PostgreSQL database.
    It has five methods: __init__, execute_query, create_schema,
    write_dataframe_to_database, and get_df_from_db.

    Args:
        db_config: str, credentials to connect to the database

    Attributes:
        engine: SQLAlchemy engine object for interacting with the database
        engine_connect: connection object for interacting with the database using the engine object
        _connect: connection object for interacting with the database using the psycopg2 library
        _cursor: cursor object for executing queries on the database
    """

    def __init__(self, user_name, user_password, host, port, database):
        """
        Create engine, connect and cursor objects to work with database

        Args:
            db_config: credentials to connect

        Returns:
            Engine, connect and cursor objects
        """
        self.connection = psycopg2.connect(user=user_name,
                                      password=user_password,
                                      host=host,
                                      port=port,
                                      database=database)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def create_schema(self, statement: str | None):
        """
        Creates database schema

        Args:
            statement: SQL query

        Returns:
            The method returns None
        """
        # creating schema and tables
        self.cursor.execute("""DROP SCHEMA task1 CASCADE""")
        self.cursor.execute("""CREATE SCHEMA IF NOT EXISTS task1""")
        self.connection.commit()
        logging.info('Schema task1 was created')
        self.cursor.execute("""SET SEARCH_PATH to task1""")
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS rooms(
                id INT,
                name VARCHAR(20));
            CREATE TABLE IF NOT EXISTS students(
                birthday TIMESTAMP,
                id INT,
                name VARCHAR(100),
                room INT,
                sex CHAR);
            """)
        logging.info('DB tables rooms and students were created')

    @staticmethod
    def write_dataframe_to_database(df: pd.DataFrame, db_config: str, table_name=None, schema=None) -> Literal[True]:
        """
        Writes DataFrame object into database

        Args:
            df: pandas DataFrame object
            db_config: credentials to connect
            table_name: name of the table, by default is None
            schema: name of the schema, by default is None

        Returns:
            True if executed
        """
        df.to_sql(
            name=table_name,
            con=db_config,
            schema=schema,
            if_exists='replace',
            index=False,
        )
        return True

    def get_df_from_db(self, statement=None) -> pd.DataFrame:
        """
        Get DataFrame object from the database using SQL query

        Args:
            statement: SQL query

        Returns:
            Pandas DataFrame object
        """
        return pd.read_sql(statement, self.engine_connect)
