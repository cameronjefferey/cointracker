
select
    case
        when price_per_item > next_sales_price then 'Increase'
        when price_per_item < next_sales_price then 'Decrease'
        when price_per_item = next_sales_price then 'No Change'
        when next_sales_price is null then 'Unknown'
    end as price_change_type,
    count(distinct asset_id) as assets
from {{ ref('next_price')}}
group by 1
