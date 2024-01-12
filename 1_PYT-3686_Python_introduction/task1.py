import psycopg2
import pandas as pd
import os
import json
from dotenv import load_dotenv
import subprocess
import logging


# dotenv lib for credentials
load_dotenv()
dbms = os.environ.get("DBMS")
user_name = os.environ.get("USER_NAME")
user_password = os.environ.get("USER_PASSWORD")
host = os.environ.get("HOST")
port = os.environ.get("PORT")
database = os.environ.get('DATABASE')


def main():
    """main function"""
    # logging settings
    logging.basicConfig(
        level=logging.INFO,                                  # logging level
        format='%(asctime)s - %(levelname)s - %(message)s',  # log message format
        filename='process.log',                              # log file name
        filemode='w')                                        # overwrite existing logs

    # connection to the db
    connection = psycopg2.connect(user=user_name,
                                  password=user_password,
                                  host=host,
                                  port=port,
                                  database=database)
    connection.autocommit = True
    cursor = connection.cursor()

    # creating schema and tables
    cursor.execute("""DROP SCHEMA task1 CASCADE""")
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS task1""")
    connection.commit()
    logging.info('Schema task1 was created')
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
    logging.info('DB tables rooms and students were created')

    # input source files names and getting path
    while True:
        filename_students = input('Enter student file name with extension (for example, students.json): ')  # students.json
        filename_rooms = input('Enter rooms file name with extension (for example, rooms.json): ')  # rooms.json
        if filename_students == "students.json" and filename_rooms == "rooms.json":
            break
        else:
            print("Please check - you should input correct file names. Try again.")

    # generating absolute path for input files
    for root, dirs, files in os.walk(os.getcwd()):
        for name in files:
            if name == filename_students:
                path_students = os.path.abspath(os.path.join(root, name))
            if name == filename_rooms:
                path_rooms = os.path.abspath(os.path.join(root, name))
    filename_query = ['rooms_with_dif_gender_v1.sql',
                      'rooms_with_max_age_diff.sql',
                      'rooms_with_min_avg_age.sql',
                      'rooms_with_students.sql']
    path_query = [os.path.join(os.getcwd(), 'queries', i) for i in filename_query]
    path_output = [os.path.join(os.getcwd(), r'source/output', i) for i in filename_query]  # for Windows change "/" to "\" in path
    # print(os.getcwd())
    # print(path_students)
    # print(path_rooms)
    # print(path_query)
    # print(path_output)

    # reading source files
    try:
        with open(path_students) as s, open(path_rooms) as r:
            data_students = s.read()
            data_rooms = r.read()
    except FileNotFoundError:
        print("File not found")
        logging.error('File not found')
    logging.info('Input files rooms and students were read')

    # import json to db tables
    cursor.execute("""
        INSERT INTO students
        SELECT *
        FROM json_populate_recordset(NULL::students, %s);
        INSERT INTO rooms
        SELECT *
        FROM json_populate_recordset(NULL::rooms, %s);
        """, (data_students, data_rooms))
    logging.info('Import json data from input files to db tables was performed')

    # creating keys
    cursor.execute("""
        ALTER TABLE rooms
            ADD PRIMARY KEY (id);
        ALTER TABLE students
            ADD FOREIGN KEY(room)
            REFERENCES rooms (id);
        """)

    # v1 using pandas to_json and to_xml
    def json_file(file_path_input: str, file_path_output: str) -> None:
        """Function to write in file in json format using pandas lib"""
        try:
            with open(file_path_input, 'r') as sql_file,\
                 open(file_path_output, 'w') as json_file:
                cursor.execute(sql_file.read())
                df = pd.DataFrame(cursor.fetchall(), columns=['room', 'result'])
                json_file.write(df.to_json(orient='records', lines=True, indent=4))
        except FileNotFoundError:
            print("File not found")
            logging.error('File not found')
        except PermissionError:
            print("Permission denied")
            logging.error("Permission denied")

    def xml_file(file_path_input: str, file_path_output: str) -> None:
        """Function to write in file in json format using pandas lib"""
        try:
            with open(file_path_input, 'r') as sql_file,\
                 open(file_path_output, 'w') as xml_file:
                cursor.execute(sql_file.read())
                df = pd.DataFrame(cursor.fetchall(), columns=['room', 'result'])
                xml_file.write(df.to_xml())
        except FileNotFoundError:
            print("File not found")
            logging.error('File not found')
        except PermissionError:
            print("Permission denied")
            logging.error("Permission denied")

    # input of required file format
    for i in range(4):
        while True:
            file_format = input(f'Input required format of #{i+1} query file (json/xml): ') # json or xml
            if file_format == "json" or file_format =="xml":
                break
            else:
                print("Please check - you should input json or xml. Try again.")
        if file_format == 'json':
            path_output[i] = path_output[i].replace('.sql', '.json')
            json_file(path_query[i], path_output[i])
        else:
            path_output[i] = path_output[i].replace('.sql', '.xml')
            xml_file(path_query[i], path_output[i])
    logging.info('Output files rooms and students were created - v1')

    # Additional export options
    # v2 using python json dump module
    # for Windows change "/" to "\" in path
    try:
        with open(r'queries/rooms_with_dif_gender_v1.sql', 'r') as sql_file, \
             open(r'source/output/rooms_with_dif_gender_v2.json', 'w') as json_file:
            cursor.execute(sql_file.read())
            fetch_result = cursor.fetchall()
            json.dump(fetch_result, json_file, indent=4)
    except FileNotFoundError:
        print("File not found")
        logging.error('File not found')
    except PermissionError:
        print("Permission denied")
        logging.error("Permission denied")
    logging.info('Output files rooms and students were created - v2')

    # v3 using json_agg(to_json(t)) inside sql script
    # specify hardcoded path only in sql
    # ! NOT WORKING IN LINUX - got error:
    # psycopg2.errors.InsufficientPrivilege: could not open file "/home/user/projects/de-trainee/1_PYT-3686_Python_introduction/source/output/rooms_with_dif_gender_v3.json" for writing: Permission denied
    # HINT: COPY TO instructs the PostgreSQL server process to write a file. You may want a client-side facility such as psql's \copy.
    # try:
    #     with open(r'queries/rooms_with_dif_gender_v2.sql', 'r') as sql_file:
    #         # Set write permission for the file
    #         file_path = '/home/user/projects/de-trainee/1_PYT-3686_Python_introduction/source/output/rooms_with_dif_gender_v3.json'
    #         os.chmod(file_path, 0o666)
    #         cursor.execute(sql_file.read())
    # except FileNotFoundError:
    #     print("File not found.")
    # except PermissionError:
    #     print("Permission denied.")
    #
    # # Set write permission for the file
    # file_path = '/home/user/projects/de-trainee/1_PYT-3686_Python_introduction/source/output/rooms_with_dif_gender_v3.json'
    # os.chmod(file_path, 0o666)
    # # Define the psql command
    # psql_command = [
    #     'psql',
    #     '-h',
    #     host,
    #     '-p',
    #     port,
    #     '-U',
    #     user_name,
    #     '-d',
    #     database,
    #     '-c',
    #     '''
    #     \\COPY (SELECT json_agg(t) FROM (  SELECT s.room
    #                                         FROM task1.students AS s
    #                                         GROUP BY s.room
    #                                         HAVING COUNT(DISTINCT s.sex)=2
    #                                         ORDER BY s.room) t
    #     )) TO \'output/rooms_with_dif_gender_v3.json\'
    #     ''',
    #     user_password,
    #     '-W',
    # ]
    # # Run the psql command
    # subprocess.run(psql_command)


if __name__ == '__main__':
    main()