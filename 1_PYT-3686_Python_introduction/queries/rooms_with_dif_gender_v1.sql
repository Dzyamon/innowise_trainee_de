--Список комнат где живут разнополые студенты
SELECT
    s.room,
    COUNT(DISTINCT s.sex) AS cnt_sex
FROM task1.students AS s
GROUP BY s.room
HAVING COUNT(DISTINCT s.sex)=2
--HAVING MIN(sex) <> MAX(sex)
ORDER BY s.room;
