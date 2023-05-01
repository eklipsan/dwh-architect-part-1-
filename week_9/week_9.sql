-- создание staging слоя
-- создание таблиц staging слоя

drop table if exists staging.film;

create table staging.film (
	film_id int not null,
	title varchar(255) not null,
	description text null,
	release_year int2 null,
	language_id int2 not null,
	rental_duration int2 not null,
	rental_rate numeric(4,2) not null,
	length int2 null,
	replacement_cost numeric(5,2) not null,
	rating varchar(10) null,
	last_update timestamp not null,
	special_features _text null,
	fulltext tsvector not null
);

drop table if exists staging.inventory;

create table staging.inventory (
	inventory_id int4 not null,
	film_id int2 not null,
	store_id int2 not null
);

drop table if exists staging.rental;

create table staging.rental (
	rental_id int4 not null,
	rental_date timestamp not null,
	inventory_id int4 not null,
	customer_id int2 not null,
	return_date timestamp null,
	staff_id int2 not null
);

drop table if exists staging.payment;

create table staging.payment (
	payment_id int4 not null,
	customer_id int2 not null,
	staff_id int2 not null,
	rental_id int4 not null,
	amount numeric(5,2) not null,
	payment_date timestamp not null
);

drop table if exists staging.staff;

create table staging.staff (
	staff_id int4 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	store_id int2 NOT NULL
);

drop table if exists staging.address;

create table staging.address (
	address_id int4 NOT NULL,
	address varchar(50) NOT NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL
);

drop table if exists staging.city;

CREATE TABLE staging.city (
	city_id int4 NOT NULL,
	city varchar(50) NOT NULL
);

drop table if exists staging.store;

CREATE TABLE staging.store (
	store_id integer NOT NULL,
	address_id int2 NOT NULL
);

-- создание процедур загрузки данных в staging слой

create or replace procedure staging.film_load()
 as $$
	begin
		delete from staging.film;

		insert
		into
		staging.film
			(film_id,
			title,
			description,
			release_year,
			language_id,
			rental_duration,
			rental_rate,
			length,
			replacement_cost,
			rating,
			last_update,
			special_features,
			fulltext)
		select 
			film_id,
			title,
			description,
			release_year,
			language_id,
			rental_duration,
			rental_rate,
			length,
			replacement_cost,
			rating,
			last_update,
			special_features,
			fulltext
		from
			public.film;
	end;
$$ language plpgsql;

create or replace procedure staging.inventory_load()
as $$
	declare 
		last_update_dt date;
	begin
		last_update_dt = coalesce(
		(
		select 
			max(update_dt) as max_date
		from staging.last_update
		where table_name = 'staging.inventory'
		),
		'1900-01-01'::date
	);
		
		truncate staging.inventory;
	
		insert into staging.inventory
		(
			inventory_id, 
			film_id, 
			store_id,
			deleted
			
		)
		select 
			inventory_id, 
			film_id, 
			store_id,
			deleted
		from
			public.inventory i
		where
			i.last_update >= last_update or 
			i.deleted >= last_update ;
		
		insert into staging.last_update (table_name, update_dt) values
		('staging.inventory', now() );
	
	end;
$$ language plpgsql;


create or replace procedure staging.rental_load()
as $$
	begin
		delete from staging.rental;

		insert into staging.rental
		(
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id
		)
		select 
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id
		from
			public.rental;
	end;

$$ language plpgsql;

create or replace procedure staging.payment_load()
as $$
	begin
		delete from staging.payment;

		insert into staging.payment
		(
			payment_id, 
			customer_id, 
			staff_id, 
			rental_id, 
			amount, 
			payment_date
		)
		select
			payment_id, 
			customer_id, 
			staff_id, 
			rental_id, 
			amount, 
			payment_date
		from
			public.payment;
	end;
$$ language plpgsql;

create or replace procedure staging.staff_load()
as $$
	begin 
		delete from staging.staff;
	
		insert into staging.staff
		(
			staff_id,
			first_name,
			last_name,
			store_id
		)
		select
			staff_id,
			first_name,
			last_name,
			store_id 
		from
			public.staff s;
	end;
$$ language plpgsql;


create or replace procedure staging.address_load()
as $$
	begin 
		delete from staging.address;
	
		insert into staging.address
		(
			address_id,
			address,
			district,
			city_id
		)
		select
			address_id,
			address,
			district,
			city_id
		from 
			public.address;
	end;
$$ language plpgsql;

create or replace procedure staging.city_load()
as $$
	begin 
		delete from staging.city;
	
		insert into staging.city
		(
			city_id,
			city
		)
		select
			city_id,
			city
		from
			public.city;

	end;
$$ language plpgsql;

create or replace procedure staging.store_load()
as $$
	begin 
		delete from staging.store;
	
		insert into staging.store
		(
			store_id,
			address_id
		)
		select
			store_id,
			address_id
		from
			public.store;

	end;
$$ language plpgsql;

-- создание тадблиц core слоя

drop table if exists core.fact_payment;
drop table if exists core.fact_rental;
drop TABLE if exists core.dim_date;
drop table if exists core.dim_inventory;
drop table if exists core.dim_staff;


create table core.dim_date
(
  date_dim_pk INT primary key,
  date_actual DATE not null,
  epoch BIGINT not null,
  day_suffix VARCHAR(4) not null,
  day_name VARCHAR(11) not null,
  day_of_week INT not null,
  day_of_month INT not null,
  day_of_quarter INT not null,
  day_of_year INT not null,
  week_of_month INT not null,
  week_of_year INT not null,
  week_of_year_iso CHAR(10) not null,
  month_actual INT not null,
  month_name VARCHAR(9) not null,
  month_name_abbreviated CHAR(3) not null,
  quarter_actual INT not null,
  quarter_name VARCHAR(9) not null,
  year_actual INT not null,
  first_day_of_week DATE not null,
  last_day_of_week DATE not null,
  first_day_of_month DATE not null,
  last_day_of_month DATE not null,
  first_day_of_quarter DATE not null,
  last_day_of_quarter DATE not null,
  first_day_of_year DATE not null,
  last_day_of_year DATE not null,
  mmyyyy CHAR(6) not null,
  mmddyyyy CHAR(10) not null,
  weekend_indr BOOLEAN not null
);

create index dim_date_date_actual_idx
  on
core.dim_date(date_actual);


create table core.dim_inventory (
	inventory_pk serial primary key,
	inventory_id integer not null unique,
	film_id integer not null,
	title varchar(255) not null,
	rental_duration int2 not null,
	rental_rate numeric(4,2) not null,
	length int2,
	rating varchar(10)
);

create table core.dim_staff (
	staff_pk serial primary key,
	staff_id integer not null,
	first_name varchar(45) not null,
	last_name varchar(45) not null,
	address varchar(50) not null,
	district varchar(20) not null,
	city_name varchar(50) not null
);

create table core.fact_payment (
	payment_pk serial primary key,
	payment_id integer not null,
	amount numeric(7,2) not null,
	payment_date_fk integer not null references core.dim_date(date_dim_pk),
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk)
);

create table core.fact_rental (
	rental_pk serial primary key,
	rental_id integer not null,
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk),
	rental_date_fk integer not null references core.dim_date(date_dim_pk),
	return_date_fk integer references core.dim_date(date_dim_pk),
	cnt int2 not null,
	amount numeric(7,2)
);

create or replace procedure core.load_date(sdate date, nm integer)
as $$
	begin
--		SET lc_time = 'ru_RU';
		
		INSERT INTO core.dim_date
		SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
		       datum AS date_actual,
		       EXTRACT(EPOCH FROM datum) AS epoch,
		       TO_CHAR(datum, 'fmDDth') AS day_suffix,
		       TO_CHAR(datum, 'TMDay') AS day_name,
		       EXTRACT(ISODOW FROM datum) AS day_of_week,
		       EXTRACT(DAY FROM datum) AS day_of_month,
		       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
		       EXTRACT(DOY FROM datum) AS day_of_year,
		       TO_CHAR(datum, 'W')::INT AS week_of_month,
		       EXTRACT(WEEK FROM datum) AS week_of_year,
		       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
		       EXTRACT(MONTH FROM datum) AS month_actual,
		       TO_CHAR(datum, 'TMMonth') AS month_name,
		       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
		       EXTRACT(QUARTER FROM datum) AS quarter_actual,
		       CASE
		           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
		           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
		           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
		           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
		           END AS quarter_name,
		       EXTRACT(YEAR FROM datum) AS year_actual,
		       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
		       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
		       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
		       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
		       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
		       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
		       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
		       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
		       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
		       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
		       CASE
		           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
		           ELSE FALSE
		           END AS weekend_indr
		FROM (SELECT sdate + SEQUENCE.DAY AS datum
		      FROM GENERATE_SERIES(0, nm - 1) AS SEQUENCE (DAY)
		      ORDER BY SEQUENCE.day) DQ
		ORDER BY 1;

	end;
$$ language plpgsql;


create or replace procedure core.load_inventory()
as $$
	begin 
		delete from core.dim_inventory
		where inventory_id in (
			select inventory_id 
			from staging.inventory
			where deleted is not null 
		);
	
		insert
			into
			core.dim_inventory
		(
			inventory_id,
			film_id,
			title,
			rental_duration,
			rental_rate,
			length,
			rating
		)
		select
			i.inventory_id,
			i.film_id,
			f.title,
			f.rental_duration,
			f.rental_rate,
			f.length,
			f.rating 
		from
			staging.inventory i
			join staging.film f using(film_id)
		where i.deleted is null
		on conflict(inventory_id) do update
		set film_id = excluded.film_id,
			title = excluded.title,
			rental_duration = excluded.rental_duration,
			rental_rate = excluded.rental_rate,
			length = excluded.length,
			rating = excluded.rating;

	end;
$$ language plpgsql;

create or replace procedure core.load_staff()
as $$
	begin 
		delete from core.dim_staff;
	
		insert into core.dim_staff
		(
			staff_id,
			first_name,
			last_name,
			address,
			district,
			city_name
		)
		select
			s.staff_id,
			s.first_name,
			s.last_name,
			a.address,
			a.district,
			c.city 
		from
			staging.staff s
			join staging.store st using (store_id)
			join staging.address a using (address_id)
			join staging.city c using (city_id);
	end;
$$ language plpgsql;

create or replace procedure core.load_payment()
as $$
	begin
		delete from core.fact_payment;
	
		insert into core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk
		)
		select
			p.payment_id,
			p.amount,
			dt.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk
		from
			staging.payment p
			join staging.rental r using (rental_id)
			join core.dim_inventory di using (inventory_id)
			join core.dim_staff ds on p.staff_id = ds.staff_id
			join core.dim_date dt on dt.date_actual = p.payment_date::date;

	end;
$$ language plpgsql;

create or replace procedure core.load_rental()
as $$
	begin 
		delete from core.fact_rental;
	
		insert into core.fact_rental
		(
			rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			amount,
			cnt
		)
		select
			r.rental_id,
			i.inventory_pk as inventory_fk,
			s.staff_pk as staff_fk,
			dt_rental.date_dim_pk as rental_date_fk,
			dt_return.date_dim_pk as return_date_fk,
			sum(p.amount) as amount,
			count(*) as cnt
		from
			staging.rental r
			join core.dim_inventory i using (inventory_id)
			join core.dim_staff s on s.staff_id = r.staff_id
			join core.dim_date dt_rental on dt_rental.date_actual = r.rental_date::date
			left join staging.payment p using (rental_id)
			left join core.dim_date dt_return on dt_return.date_actual = r.return_date::date
		group by
			r.rental_id,
			i.inventory_pk,
			s.staff_pk,
			dt_rental.date_dim_pk,
			dt_return.date_dim_pk;

	end
$$ language plpgsql;


create or replace procedure core.fact_delete()
as $$
	begin
		delete from core.fact_payment;
		delete from core.fact_rental;
	end
$$ language plpgsql;

-- создание data mart слоя

drop schema if exists report;
create schema report;

drop table if exists report.sales_date;

create table report.sales_date (
	date_title varchar(20) not null,
	amount numeric(7,2) not null,
	date_sort integer not null
);

create or replace procedure report.sales_date_calc()
as $$
	begin 
		delete from report.sales_date;
	
		insert
			into
			report.sales_date
		(
			date_title, --'1 сентября 2022'
			amount,
			date_sort
		)
		select
			dt.day_of_month || ' ' || dt.month_name || ' ' || dt.year_actual as date_title,
			sum(fp.amount) as amount,
			dt.date_dim_pk as date_sort
		from
			core.fact_payment fp
			join core.dim_date dt
				on	 fp.payment_date_fk = dt.date_dim_pk
		group by
			dt.day_of_month || ' ' || dt.month_name || ' ' || dt.year_actual,
			dt.date_dim_pk;

	end
$$ language plpgsql;



--

create or replace procedure full_load()
as $$
	begin
		call staging.film_load();
		call staging.inventory_load();
		call staging.rental_load();
		call staging.payment_load();
		call staging.staff_load();
		call staging.address_load();
		call staging.city_load();
		call staging.store_load();
		
		
		call core.fact_delete();
		call core.load_inventory();
		call core.load_staff();
		call core.load_payment();
		call core.load_rental();
	
		call report.sales_date_calc();
	end;
$$ language plpgsql;


call core.load_date('2007-01-01'::date, 5843);
call full_load();