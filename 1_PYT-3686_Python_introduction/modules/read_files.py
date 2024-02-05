import os
from logger.logger_func import logging_task1 as lgr


def read_input_task1():
    """
    Get source files names and find their paths.
    Read source files.
    Returns:
        data_students, data_rooms, path_query, path_output
    """
    while True:
        filename_students = input('Enter student file name with extension (for example, students.json): ')
        filename_rooms = input('Enter rooms file name with extension (for example, rooms.json): ')
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
    path_output = [os.path.join(os.getcwd(), r'source/output', i) for i in filename_query]  # for Windows change "/" to "\"
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
        lgr(40, 'File not found')

    lgr(20, 'Input files rooms and students were read')

    return data_students, data_rooms, path_query, path_output