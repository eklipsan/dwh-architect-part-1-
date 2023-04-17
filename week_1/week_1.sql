 -- 1 task
 drop function if exists count_disk;
 create function count_disk(film_id int) returns int
 as $$
 	select count(inventory_id)
 	from inventory i 
 	where film_id = count_disk.film_id
 $$ language sql;

drop function if exists count_rent;
create function count_rent(film_id int) returns int
as $$
	select count(rental_id)
	from rental r join inventory i using(inventory_id)
	join film using(film_id)
	where i.film_id = count_rent.film_id
$$ language sql;

 select
 	f.title,
 	count_disk(f.film_id),
 	count_rent(f.film_id) 	
 from 
 	film f
 	
 
 -- 2 task
 drop function if exists top_num;
 create function top_num(x int, y int) returns int
 as $$
 	select
 		case 
 			when x > y then x
 			else y
 		end
 $$ language sql;
 select top_num(2,5);
 

-- 3 task
drop function if exists new_disk;
create function new_disk(film_id int, store_id int) returns varchar(5)
as $$
	insert into inventory (film_id,store_id) values 
	(new_disk.film_id, new_disk.store_id);

	select $text$DONE!$text$
$$ language sql;

select new_disk(10,1);
select new_disk(15,3);
select new_disk(20,5);


-- 4 task
drop function if exists film_revenue;
create function film_revenue(film_id int) returns table(payment_date date,revenue decimal(10,2))
as $$
	select 
	date(payment_date),
	sum(amount)
	from payment p join rental using(rental_id)
	join inventory using(inventory_id)
	join film using(film_id)
	where film.film_id = film_revenue.film_id
	group by 1
	having sum(amount) > 0
	order by 1 asc
$$ language sql;
select * from film_revenue(1)