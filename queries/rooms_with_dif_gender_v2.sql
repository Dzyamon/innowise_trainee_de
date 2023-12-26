--Список комнат где живут разнополые студенты
COPY (
    SELECT
        json_agg(to_json(t))
    FROM (
        SELECT
            s.room
            --COUNT(DISTINCT s.sex) AS cnt_sex
        FROM task1.students AS s
        GROUP BY s.room
        HAVING COUNT(DISTINCT s.sex)=2
        --HAVING MIN(sex) <> MAX(sex)
        ORDER BY s.room) t
    )
--SELECT json_agg(t) FROM t \g my_data_dump.json;
TO 'D:\DO\Innowise\trainee\task1\source\output\rooms_with_dif_gender_v3.json';
