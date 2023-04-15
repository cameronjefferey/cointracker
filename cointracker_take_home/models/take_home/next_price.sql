
select
    nft_sales.*,
    lead(price_per_item) over (partition by asset_id order by event_time) as next_sales_price,
    lead(event_time) over (partition by asset_id order by event_time) as next_sales_date
from {{ ref('nft_sales')}}
