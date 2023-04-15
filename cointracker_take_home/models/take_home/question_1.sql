select
    number_day_of_week_event,
    written_day_of_week_event,
    avg(price_per_item) as avg_sales_price
from {{ ref('nft_sales')}}
group by 1,2
order by 3
