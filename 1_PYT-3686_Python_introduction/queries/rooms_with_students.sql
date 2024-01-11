--Список комнат и количество студентов в каждой из них
SELECT
	r.name,
	COUNT(*)
FROM
	task1.students AS s
	JOIN task1.rooms AS r ON s.room = r.id
GROUP BY r.name
ORDER BY r.name;