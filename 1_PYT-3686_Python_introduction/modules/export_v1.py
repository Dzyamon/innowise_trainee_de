from modules.json_file import json_file_task1
from modules.xml_file import xml_file_task1


def file_export_v1_task1(cursor, path_query, path_output):
    """
    Input of required file format and generate full paths for files.
    Perform export of json and xml files.
    Args:
        cursor: cursor object
        path_query: full path of queries files with extension
        path_output: full path of output files with extension
    """
    for i in range(4):
        while True:
            file_format = input(f'Input required format of #{i+1} query file (json/xml): ')
            if file_format == "json" or file_format =="xml":
                break
            else:
                print("Please check - you should input json or xml. Try again.")
        if file_format == 'json':
            path_output[i] = path_output[i].replace('.sql', '.json')
            json_file_task1(cursor, path_query[i], path_output[i])
        else:
            path_output[i] = path_output[i].replace('.sql', '.xml')
            xml_file_task1(cursor, path_query[i], path_output[i])
