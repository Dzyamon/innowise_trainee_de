def db_create_task1(cursor):
    """
    Create schema and tables
    Args:
        cursor object
    """
    cursor.execute("""DROP SCHEMA task1 CASCADE""")
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS task1""")
    #connection.commit()
    cursor.execute("""SET SEARCH_PATH to task1""")
    cursor.execute("""
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
