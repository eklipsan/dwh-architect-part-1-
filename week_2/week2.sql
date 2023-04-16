-- task 1
create or replace procedure coal(in x anyelement, inout y anyelement) as
$$ 
    select case
        when x is not null then x 
        when y is not null then y
        end
$$ language sql;

call coal(1,2);
call coal(2,null);

-- task 2
create or replace procedure coal(in x anyelement, in y anyelement, inout z anyelement) as
$$ 
    select case
        when x is not null then x
        when y is not null then y
        when z is not null then z
        end
$$ language sql;

call coal(2,3);


-- task 3
create or replace procedure coal(in x anyelement, in y anyelement, inout z anyelement) as
$$
-- taking the first not-null value out of the 3 parameters
    select case
        when x is not null then x
        when y is not null then y
        when z is not null then z
        else 'all are null'
        end
$$ language sql;

-- task 4
create table floatik(
    x float not null
);

select * from floatik;


create or replace procedure fill_floatik(inout x integer) as
$$ 
    insert into floatik select 
        random() 
        from generate_series(1,x);

    select avg(x) from floatik;
$$ language sql;

call fill_floatik(3);

select * from floatik;

drop table floatik;

-- task 5
create or replace procedure fill_rental(in nm integer)
as $$ 
insert into rental(inventory_id,rental_date,customer_id,staff_id) select 
	round(random() * (select max(inventory_id) from rental)),
	(select max(rental_date) + ('2122' * interval '3 day') from rental),
	round(random() * (select max(customer_id) from rental)),
	round(random() * (select max(staff_id) + 1 from staff))
from generate_series(1,3)
$$ language sql;

--select round(random() * (select max(inventory_id) from rental))

--select max(rental_date) + ('2122' * interval '3 day') from rental