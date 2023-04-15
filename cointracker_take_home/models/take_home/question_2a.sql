with next_price as (
select
    nft_sales.*,
    lead(price_per_item) over (partition by asset_id order by event_time) as next_sales_price,
    lead(event_time) over (partition by asset_id order by event_time) as next_sales_date
from {{ ref('nft_sales')}}
)

select
    case
        when price_per_item > next_sales_price then 'Increase'
        when price_per_item < next_sales_price then 'Decrease'
        when price_per_item = next_sales_price then 'No Change'
        when next_sales_price is null then 'Unknown'
    end as price_change_type,
    count(distinct asset_id) as assets
from next_price
group by 1
