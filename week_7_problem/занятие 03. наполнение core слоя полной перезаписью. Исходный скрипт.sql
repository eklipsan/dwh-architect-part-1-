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
			film_src.film;
	end;
$$ language plpgsql;

create or replace procedure staging.inventory_load()
as $$
	begin
		delete from staging.inventory;

		insert into staging.inventory
		(
			inventory_id, 
			film_id, 
			store_id
			
		)
		select 
			inventory_id, 
			film_id, 
			store_id
		from
			film_src.inventory i;
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
			film_src.rental;
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
			film_src.payment;
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
			film_src.staff s;
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
			film_src.address;
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
			film_src.city;

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
			film_src.store;

	end;
$$ language plpgsql;

-- создание тадблиц core слоя

drop table if exists core.fact_payment;
drop table if exists core.fact_rental;
drop table if exists core.dim_inventory;
drop table if exists core.dim_staff;

create table core.dim_inventory (
	inventory_pk serial primary key,
	inventory_id integer not null,
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
	payment_date date not null,
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk)
);

create table core.fact_rental (
	rental_pk serial primary key,
	rental_id integer not null,
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk),
	rental_date date not null,
	return_date date,
	cnt int2 not null,
	amount numeric(7,2)
);
