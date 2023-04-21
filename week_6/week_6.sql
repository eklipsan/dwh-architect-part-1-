drop schema if exists core;
create schema core;

drop table if exists dim_staff;
create table core.dim_staff (
	staff_pk integer not null,
	
	staff_id int4 not null,
	first_name varchar(45) not null,
	last_name varchar(45) not null,
	address_id int2 not null,
	email varchar(50),
	store_id int2 not null,
	active bool not null,
	username varchar(16) not null,
	password varchar(40),
	last_update date,
	
	primary key(staff_pk)
);


drop table if exists fact_rental;
create table core.fact_rental (
	fact_rental_pk integer not null,
	staff_fk integer not null,
	
	rental_id int4 not null,
	rental_date date not null,
	inventory_id int4 not null,
	customer_id int2 not null,
	return_date date,
	staff_id int2 not null,
	last_update date,
	
	foreign key (staff_fk) references core.dim_staff(staff_pk)
);
	