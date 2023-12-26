--5 комнат с самой большой разницей в возрасте студентов
SELECT
	s.room,
	MAX(CURRENT_DATE - DATE(s.birthday)) - MIN(CURRENT_DATE - DATE(s.birthday)) AS age_diff
--	MAX(CURRENT_DATE - DATE(s.birthday)) AS age_max,
--	MIN(CURRENT_DATE - DATE(s.birthday)) AS age_min
FROM task1.students AS s
GROUP BY s.room
ORDER BY age_diff DESC
LIMIT 5;