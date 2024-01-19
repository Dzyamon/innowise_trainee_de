import pandas as pd
from logger.logger_func import logging_task1 as lgr


def xml_file_task1(cursor, file_path_input: str, file_path_output: str) -> None:
    """
    Write data in output file in xml format using pandas library
    Args:
        cursor: cursor object
        file_path_input: str, input path
        file_path_output: str, output path
    """
    try:
        with open(file_path_input, 'r') as sql_file,\
             open(file_path_output, 'w') as xml_file:
            cursor.execute(sql_file.read())
            df = pd.DataFrame(cursor.fetchall(), columns=['room', 'result'])
            xml_file.write(df.to_xml())
    except FileNotFoundError:
        print("File not found")
        lgr(40, 'File not found')
    except PermissionError:
        print("Permission denied")
        lgr(40, "Permission denied")
