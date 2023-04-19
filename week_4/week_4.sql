CREATE
	OR replace FUNCTION film_details_by_actor (actor_last_name VARCHAR(45))
RETURNS TABLE (
		rating mpaa_rating
		,film_nm INT
		,amount_avg_per_film FLOAT
		,disk_avg_per_film FLOAT
		) AS $$
BEGIN
	CREATE TEMPORARY TABLE film_with_actor ON COMMIT DROP AS
	SELECT f.film_id
		,f.rating
	FROM film f
	INNER JOIN film_actor using (film_id)
	INNER JOIN actor using (actor_id)
	WHERE actor.last_name = film_details_by_actor.actor_last_name;

	CREATE TEMPORARY TABLE film_nm_by_rating ON	COMMIT DROP AS
	SELECT f.rating
		,count(f.film_id) AS film_nm
	FROM film_with_actor f
	GROUP BY f.rating;

	CREATE TEMPORARY TABLE amount_and_disk_nm_per_rating ON	COMMIT DROP AS SELECT fa.rating
		,sum(p.amount) AS total_amount
		,count(DISTINCT i.inventory_id) AS total_disk
	FROM film_with_actor fa
	INNER JOIN inventory i using (film_id)
	LEFT JOIN rental r using (inventory_id)
	LEFT JOIN payment p using (rental_id)
	GROUP BY fa.rating;

	RETURN query
	SELECT fr.rating
		,fr.film_nm::INT
		,cast(ar.total_amount / fr.film_nm AS FLOAT) AS amount_avg_per_film
		,cast(ar.total_disk / fr.film_nm AS FLOAT) AS disk_avg_per_film
	FROM film_nm_by_rating fr
	LEFT JOIN amount_and_disk_nm_per_rating ar using (rating);
END;
$$ LANGUAGE plpgsql;


SELECT *
FROM actor a;

SELECT *
FROM film_details_by_actor('Guiness');

DROP FUNCTION film_details_by_actor;