import json
from logger.logger_func import logging_task1 as lgr


def file_export_v2_task1(cursor):
    """
    Additional version to perform export to json format using json dump lib
    for Windows change "/" to "\" in path
    """
    try:
        with open(r'queries/rooms_with_dif_gender_v1.sql', 'r') as sql_file, \
             open(r'source/output/rooms_with_dif_gender_v2.json', 'w') as json_file:
            cursor.execute(sql_file.read())
            fetch_result = cursor.fetchall()
            json.dump(fetch_result, json_file, indent=4)
    except FileNotFoundError:
        print("File not found")
        lgr(40, 'File not found')
    except PermissionError:
        print("Permission denied")
        lgr(40, "Permission denied")
