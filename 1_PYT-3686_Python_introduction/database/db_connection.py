import psycopg2


def db_connection_task1(user_name, user_password, host, port, database):
    """
    Establish connection to the DB
    Args:
        user_name, user_password, host, port, database: str, connection credentials for the database
    Returns:
        cursor object
    """
    connection = psycopg2.connect(user=user_name,
                                  password=user_password,
                                  host=host,
                                  port=port,
                                  database=database)
    connection.autocommit = True
    cursor = connection.cursor()
    return cursor
