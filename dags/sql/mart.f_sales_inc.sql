with cte_f_sales as (
    select 
        dc.date_id, 
        item_id, 
        customer_id, 
        city_id, 
        quantity, 
        case when uol.status='refunded'  then -payment_amount else payment_amount end as payment_amount
    from staging.user_order_log uol
        left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
    where uol.date_time::Date = '{{ds}}'
)
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select
    date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    payment_amount
from cte_f_sales
on conflict on constraint uc_f_sales 
do update set
	quantity = EXCLUDED.quantity,
    payment_amount = EXCLUDED.payment_amount;