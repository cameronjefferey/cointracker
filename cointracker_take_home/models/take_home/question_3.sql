select
    collection_name,
    count(*) as trades,
    avg(price_per_item) as avg_price_per_item
from {{ ref('nft_sales')}}
group by 1
order by 2 desc
