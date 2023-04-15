
select
    date_trunc(asset_contract_date,year) as year,
    avg(price_per_item) as avg_price,
    count(*) as trades
from {{ ref('nft_sales')}}
group by 1
order by 1
