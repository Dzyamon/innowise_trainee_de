def db_keys_task1(cursor):
    """
    Create keys
    """
    cursor.execute("""
        ALTER TABLE rooms
            ADD PRIMARY KEY (id);
        ALTER TABLE students
            ADD FOREIGN KEY(room)
            REFERENCES rooms (id);
        """)
