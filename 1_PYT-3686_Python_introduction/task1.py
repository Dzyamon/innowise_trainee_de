import psycopg2
import pandas as pd
import os
import json


if __name__ == '__main__':
    # connection to the db
    connection = psycopg2.connect(user="postgres",
                                  password="mypostgresql86",
                                  host="localhost",
                                  port="5432",
                                  database="postgres")
    connection.autocommit = True
    cursor = connection.cursor()

    # creating schema and tables
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS task1""")
    connection.commit()
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

    # input source files names and getting path
    filename_students = input('Enter student file name with extension: ')   # students.json
    filename_rooms = input('Enter rooms file name with extension: ')    # rooms.json
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
    path_output = [os.path.join(os.getcwd(), r'source\output', i) for i in filename_query]
    # print(os.getcwd())
    # print(path_students)
    # print(path_rooms)
    # print(path_query)
    # print(path_output)

    # reading source files
    with open(path_students) as s, open(path_rooms) as r:
        data_students = s.read()
        data_rooms = r.read()

    # import json to db tables
    cursor.execute("""
    INSERT INTO students
    SELECT *
    FROM json_populate_recordset(NULL::students, %s);
    INSERT INTO rooms
    SELECT *
    FROM json_populate_recordset(NULL::rooms, %s);
    """, (data_students, data_rooms))

    # creating keys
    cursor.execute("""
    ALTER TABLE rooms
        ADD PRIMARY KEY (id);
    ALTER TABLE students
        ADD FOREIGN KEY(room)
        REFERENCES rooms (id);
    """)

    # v1 using pandas to_json and to_xml
    def json_file(file_path_input, file_path_output):
        with open(file_path_input, 'r') as sql_file,\
             open(file_path_output, 'w') as json_file:
            cursor.execute(sql_file.read())
            df = pd.DataFrame(cursor.fetchall(), columns=['room', 'result'])
            json_file.write(df.to_json(orient='records', lines=True, indent=4))
    def xml_file(file_path_input, file_path_output):
        with open(file_path_input, 'r') as sql_file,\
             open(file_path_output, 'w') as xml_file:
            cursor.execute(sql_file.read())
            df = pd.DataFrame(cursor.fetchall(), columns=['room', 'result'])
            xml_file.write(df.to_xml())

    # input of required file format
    for i in range(4):
        file_format = input(f'Input required format of #{i+1} query file (json/xml): ') # json or xml
        if file_format == 'json':
            path_output[i] = path_output[i].replace('.sql', '.json')
            json_file(path_query[i], path_output[i])
        else:
            path_output[i] = path_output[i].replace('.sql', '.xml')
            xml_file(path_query[i], path_output[i])

    # Additional export options
    # v2 using python json dump module
    with open(r'queries\rooms_with_dif_gender_v1.sql', 'r') as sql_file, \
         open(r'source\output\rooms_with_dif_gender_v2.json', 'w') as json_file:
        cursor.execute(sql_file.read())
        fetch_result = cursor.fetchall()
        json.dump(fetch_result, json_file, indent=4)
    # v3 using json_agg(to_json(t)) inside sql script
    with open(r'queries\rooms_with_dif_gender_v2.sql', 'r') as sql_file:
        cursor.execute(sql_file.read())
