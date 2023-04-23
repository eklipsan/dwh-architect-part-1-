drop materialized view if exists report.report_films;
create  materialized view report.report_films as (
	select
		title as film_title,
		coalesce(sum(amount),0) as amount
	from core.dim_inventory left join core.fact_payment
	on inventory_fk = inventory_pk
	group by title
	order by sum(amount) desc
);


select * from report.report_films;