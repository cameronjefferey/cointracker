with price_change as (
select
    *,
    case
        when price_per_item > next_sales_price then 'Increase'
        when price_per_item < next_sales_price then 'Decrease'
        when price_per_item = next_sales_price then 'No Change'
        when next_sales_price is null then 'Unknown'
    end as price_change_type,
from {{ ref('next_price')}}
)
select
    date_diff(next_sales_date,event_time,day) as days,
    count(distinct asset_id) as assets,
    avg(price_per_item) as avg_price
from price_change
where price_change_type = 'Increase'
group by 1
