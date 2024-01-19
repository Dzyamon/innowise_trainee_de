from dotenv import load_dotenv
import os


def db_config_task1():
    """
    Assign variables for configuring connection with DB using dotenv library
    """
    load_dotenv()
    dbms = os.environ.get("DBMS")
    user_name = os.environ.get("USER_NAME")
    user_password = os.environ.get("USER_PASSWORD")
    host = os.environ.get("HOST")
    port = os.environ.get("PORT")
    database = os.environ.get('DATABASE')
    return user_name, user_password, host, port, database
