from database import db_config, db_connection, db_create, db_keys
from modules import read_files, import_files, export_v1, export_v2
from logger.logger_func import logging_task1 as lgr


def main():
    lgr(20, 'Start program')

    data = db_config.db_config_task1()
    cursor = db_connection.db_connection_task1(*data)
    db_create.db_create_task1(cursor)
    lgr(20, 'Schema task1 and tables rooms and students were created')

    data_students, data_rooms, path_query, path_output = read_files.read_input_task1()

    import_files.import_files_task1(cursor, data_students, data_rooms)
    lgr(20, 'Import json data from input files to db tables was performed')

    db_keys.db_keys_task1(cursor)
    lgr(20, 'Keys between tables were created')

    export_v1.file_export_v1_task1(cursor, path_query, path_output)
    lgr(20, 'Output files rooms and students were created - v1')

    export_v2.file_export_v2_task1(cursor)
    lgr(20, 'Output files rooms and students were created - v2')

    lgr(20, 'Finish program')


if __name__ == '__main__':
    main()
