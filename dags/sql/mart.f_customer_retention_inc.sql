with cte1_f_customer_retention as (
	select 
		fs2.id,
		fs2.customer_id,
		dc.date_actual,
		min(dc.date_actual) over (partition by fs2.customer_id) min_date,
		count(id) over (partition by fs2.customer_id, dc.week_of_year) as pay_cnt_of_week,
		dc.first_day_of_week,
		dc.last_day_of_week,
		dc.week_of_year,
		dc.year_actual,
		fs2.item_id,
		fs2.quantity,
		fs2.payment_amount,
		fs2.payment_amount * fs2.quantity as pay
		from mart.f_sales fs2 
			left join mart.d_calendar dc on fs2.date_id = dc.date_id 
		where dc.date_actual::Date = '{{ds}}'
	)
, cte2_f_customer_retention as (
select 
	count(distinct c1.customer_id) filter (where c1.date_actual = c1.min_date) as new_customers_count,
	count(distinct c1.customer_id) filter (where c1.pay_cnt_of_week=2) as returning_customers_count,
	count(distinct c1.customer_id) filter (where c1.payment_amount < 0) as refunded_customer_count,
	concat(to_char(c1.first_day_of_week, 'DD.MM.YYYY'), ' - ', to_char(c1.last_day_of_week, 'DD.MM.YYYY')) as period_name, 
	c1.week_of_year as period_id,
	c1.item_id,
	sum(c1.pay) filter (where c1.date_actual = c1.min_date) as new_customers_revenue,
	sum(c1.pay) filter (where c1.pay_cnt_of_week=2) as returning_customers_revenue,
	count(distinct c1.customer_id) filter (where c1.payment_amount < 0) as customers_refunded
from cte1_f_customer_retention as c1
GROUP BY
        c1.item_id,
        c1.week_of_year,
        c1.year_actual,
        c1.first_day_of_week,
        c1.last_day_of_week)
insert into mart.f_customer_retention (new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_name,
	period_id,
	item_id,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded)
select new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_name,
	period_id,
	item_id,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded
from cte2_f_customer_retention
on conflict on constraint f_customer_retention_pk 
do update set
	new_customers_count = EXCLUDED.new_customers_count,
	returning_customers_count = EXCLUDED.returning_customers_count,
	refunded_customer_count = EXCLUDED.refunded_customer_count,
	new_customers_revenue = EXCLUDED.new_customers_revenue,
	returning_customers_revenue = EXCLUDED.returning_customers_revenue,
	customers_refunded = EXCLUDED.customers_refunded