alter table staging.inventory add column
deleted timestamp null;


alter table public.inventory add column
deleted timestamp options(column_name 'deleted') null;

alter table core.dim_inventory 
add constraint inventory_id_unique unique(inventory_id)

create table staging.last_update (
	table_name varchar(50) not null,
	update_dt timestamp not null
);