-- 1.Вывести количество фильмов в каждой категории, отсортировать по убыванию.
-- отличия Sports=74 	Drama=62	Travel=57
SELECT c.name, COUNT(*) AS films_count
FROM category c
LEFT JOIN film_category fc ON fc.category_id = c.category_id
GROUP BY 1
ORDER BY 2 DESC;
-- v2 (через view "film_list")
-- отличия Sports=73	Drama=61	Travel=56  - мб из-за group на group ?
SELECT category, COUNT(*) AS films_count
FROM film_list
GROUP BY 1
ORDER BY 2 DESC;


-- 2.Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
SELECT concat(a.first_name || ' '::text || a.last_name) AS actor_name, count(inventory_id) AS rental_count
FROM actor a
JOIN film_actor fa USING (actor_id) 	-- ON fa.actor_id = a.actor_id
-- JOIN film f ON fa.film_id = f.film_id
JOIN inventory i USING(film_id) 		-- ON i.film_id = fa.film_id
JOIN rental r USING(inventory_id) 		-- ON r.inventory_id = i.inventory_id
GROUP BY 1
ORDER BY rental_count DESC
LIMIT 10;


-- 3.Вывести категорию фильмов, на которую потратили больше всего денег.
-- ??? если потратили денег на производство фильма, то таких данных нет
-- если потратили деньги на аренду, то есть готовый view sales_by_film_category
SELECT * FROM sales_by_film_category;
-- вывод одной категории с макс. арендной суммой
SELECT c.name AS category, sum(p.amount) AS total_sales
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY (sum(p.amount)) DESC
LIMIT 1;


-- 4.Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
SELECT f.title, f.film_id
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.film_id IS NULL;
--v2
SELECT f.title, f.film_id
FROM film f
WHERE NOT EXISTS (
				SELECT 1
                FROM inventory i
                WHERE i.film_id = f.film_id
                )


-- 5.Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
WITH actor_film_counts AS (
  SELECT a.first_name, a.last_name, c.name, COUNT(*) AS film_count
  FROM actor a
  JOIN film_actor fa ON a.actor_id = fa.actor_id
  JOIN film f ON fa.film_id = f.film_id
  JOIN film_category fc ON f.film_id = fc.film_id
  JOIN category c ON fc.category_id = c.category_id
  WHERE c.name = 'Children'
  GROUP BY 1,2,3
)
SELECT first_name, last_name, film_count
FROM actor_film_counts
WHERE film_count IN (
    SELECT film_count
    FROM actor_film_counts
    ORDER BY film_count DESC
    LIMIT 3)
ORDER BY film_count DESC;


-- 6.Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
SELECT c.city,
       COUNT(CASE WHEN cu.active = 1 THEN 1 END) AS active_customers,
       COUNT(CASE WHEN cu.active = 0 THEN 1 END) AS inactive_customers
FROM city c
JOIN address a ON c.city_id = a.city_id
JOIN customer cu ON a.address_id = cu.address_id
GROUP BY c.city
ORDER BY inactive_customers DESC;


-- 7.Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”.
-- То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
WITH max_rental_hours AS (
	SELECT
	    c.name AS category_name,
	    ct.city AS city_name,
	    COALESCE(SUM(EXTRACT(DAY FROM (r.return_date - r.rental_date))), 0) AS total_rental_hours
	FROM
	    category c
	    JOIN film_category fc USING (category_id)
	    JOIN inventory i USING (film_id)
	    JOIN rental r USING (inventory_id)
	    JOIN customer cu USING (customer_id)
	    JOIN address a USING (address_id)
	    JOIN city ct USING (city_id)
	WHERE
		ct.city LIKE 'A%' OR ct.city LIKE '%-%'
	GROUP BY
        1,2
	)
SELECT
    category_name,
    city_name,
    total_rental_hours
FROM
    max_rental_hours
WHERE
    total_rental_hours = (
        SELECT MAX(total_rental_hours)
        FROM max_rental_hours
        WHERE city_name LIKE 'A%'
		)
UNION ALL
SELECT
    category_name,
    city_name,
    total_rental_hours
FROM
    max_rental_hours
WHERE
    total_rental_hours = (
        SELECT MAX(total_rental_hours)
        FROM max_rental_hours
        WHERE city_name LIKE '%-%'
		)
