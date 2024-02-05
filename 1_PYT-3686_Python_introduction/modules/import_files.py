def import_files_task1(cursor, data_students, data_rooms):
    """
    Import source files in json format to created DB tables.
    Args:
        cursor: cursor object
        data_students: context of student.json
        data_rooms: context of rooms.json
    """
    cursor.execute("""
        INSERT INTO students
        SELECT *
        FROM json_populate_recordset(NULL::students, %s);
        INSERT INTO rooms
        SELECT *
        FROM json_populate_recordset(NULL::rooms, %s);
        """, (data_students, data_rooms))
