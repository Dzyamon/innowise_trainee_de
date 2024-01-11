--5 комнат, где самый маленький средний возраст студентов
SELECT
	s.room,
	ROUND(AVG(CURRENT_DATE - DATE(s.birthday))) AS age_days
--	AGE(CURRENT_DATE, task1.students.birthday) AS age_detail
FROM
    task1.students AS s
    JOIN task1.rooms AS r ON s.room = r.id
GROUP BY s.room
ORDER BY age_days
LIMIT 5;