drop schema if exists staging_layer;
create schema staging_layer;


--select * from pg_catalog.pg_available_extensions;
create extension postgres_fdw;

create server dvd_source foreign data wrapper postgres_fdw options (
	host 'localhost',
	dbname 'postgres',
	port '5432'
);

create user mapping for postgres server dvd_source options (
	user 'postgres',
	password 'admin'
);

	
drop type if exists mpaa_rating;
CREATE TYPE public."mpaa_rating" AS ENUM (
	'G',
	'PG',
	'PG-13',
	'R',
	'NC-17');

DROP DOMAIN if exists public."year";
CREATE DOMAIN public."year" AS integer
	CONSTRAINT year_check CHECK (VALUE >= 1901 AND VALUE <= 2155);

import foreign schema public from server dvd_source into public;


drop table if exists film_stage;
CREATE TABLE staging_layer.film_stage (
	film_id serial4 NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	release_year public."year" NULL,
	language_id int2 NOT NULL,
	rental_duration int2 NOT NULL,
	rental_rate numeric(4, 2) NOT NULL,
	length int2 NULL,
	replacement_cost numeric(5, 2) NOT NULL,
	rating public."mpaa_rating" NULL DEFAULT 'G'::mpaa_rating,
	last_update timestamp NOT NULL,
	special_features _text NULL,
	fulltext tsvector NOT NULL
);


drop table if exists inventory_stage;
CREATE TABLE staging_layer.inventory_stage (
	inventory_id serial4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp NOT NULL
);


drop table if exists rental_stage;
CREATE TABLE staging_layer.rental_stage (
	rental_id serial4 NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp NOT null
);


drop table if exists payment_stage;
create table staging_layer.payment_stage (
	payment_id serial4 NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamp NOT null
)

drop procedure if exists fill_film;
create or replace procedure fill_film() as 
$$
	begin
		truncate staging_layer.film_stage;
		insert into staging_layer.film_stage (
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
		fulltext) select 
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
		fulltext tsvector
		from public.film;
	end;
$$ language plpgsql;

call fill_film();


drop procedure if exists fill_inventory;
create or replace procedure fill_inventory() as
$$
	begin
		truncate staging_layer.inventory_stage;
		insert into staging_layer.inventory_stage (
		inventory_id,
		film_id,
		store_id,
		last_update
		) select 
		inventory_id,
		film_id,
		store_id,
		last_update
		from public.inventory;
	end;
$$ language plpgsql;

call fill_inventory();


drop procedure if exists fill_payment;
create or replace procedure fill_payment() as 
$$
	begin 
		truncate staging_layer.payment_stage;
		insert into staging_layer.payment_stage (
		payment_id,
		customer_id,
		staff_id,
		rental_id,
		amount,
		payment_date
		) select
		payment_id,
		customer_id,
		staff_id,
		rental_id,
		amount,
		payment_date
		from public.payment;
	end;
$$ language plpgsql;

call fill_payment();


drop procedure if exists fill_rental;
create or replace procedure fill_rental() as 
$$
	begin 
		truncate staging_layer.rental_stage;
		insert into staging_layer.rental_stage (
		rental_id,
		rental_date,
		inventory_id,
		customer_id,
		return_date,
		staff_id,
		last_update
		) select 
		rental_id,
		rental_date,
		inventory_id,
		customer_id,
		return_date,
		staff_id,
		last_update
		from public.rental;
	end;	
$$ language plpgsql;

call fill_rental();
