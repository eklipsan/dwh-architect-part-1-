-- task 1
create or replace function mul(num integer) returns integer as 
$$
	begin 
		return num * 5;
	end;	
$$ language plpgsql;

select mul(5);

-- task 2
create or replace function wake_up() returns varchar(50) as 
$$
	declare 
		hour integer := extract(hour from current_time);
	begin
		if hour < 5 then return 'Вы проснулись до 5 утра';
		else return 'Вы проснулись после 5 утра';
		end if;
	end;	
$$ language plpgsql;

select wake_up();

-- task 3
create or replace function get_film_by_duration(num float) returns integer as 
$$
	declare 
		i record;
	begin
		for i in (
		select title from film
		where rental_duration = num 
		) loop
			raise notice '%',i;
		end loop;		
		return 1;
	end;	
$$ language plpgsql;

select get_film_by_duration(6);

--task 4

create or replace function get_film_by_str(part varchar(255)) returns integer as 
$$
	declare
		i record;
	begin
		for i in (
			select title
			from film
			where title like concat('%',part,'%')
			) loop 
				raise notice '%',i;				
			end loop;
		return 1;			
	end;	
$$ language plpgsql;


select get_film_by_str('ata')









