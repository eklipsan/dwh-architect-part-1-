create or replace
procedure public.fill_rent(nm integer,
dt date default null::date)
 language sql
as $procedure$

	insert
	into
	rental (
		rental_date,
		inventory_id,
		customer_id,
		return_date,
		staff_id
	
	)
	select 
		subquery.rental_date,
		subquery.inventory_id,
		subquery.customer_id,
		subquery.rental_date + (
	select
				f.rental_duration
	from
				film f
	join inventory i
			using (film_id)
	where
				i.inventory_id = subquery.inventory_id
		
		) as return_date,
		subquery.staff_id
from
		(
	select
				coalesce(dt, (select max(rental_date)::date + 1 from rental)) rental_date,
				(
		select
						floor(rand.rand * count(*)) + 1
		from
						inventory
			
				) inventory_id,
				(
		select
						floor(rand.rand * count(*)) + 1
		from
						customer
				) customer_id,
				(
		select
						floor(rand.rand * count(*)) + 1
		from
						staff
				) staff_id
	from
			(
		select
					random() rand
		from
					generate_series(1, nm)
	
			) rand
	
	) subquery;

$procedure$;