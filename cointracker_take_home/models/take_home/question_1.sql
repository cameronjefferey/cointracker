select
    EXTRACT(dayofweek from event_time) as dow,
    case
        when EXTRACT(dayofweek from event_time) = 1 then 'Sunday'
        when EXTRACT(dayofweek from event_time) = 2 then 'Monday'
        when EXTRACT(dayofweek from event_time) = 3 then 'Tuesday'
        when EXTRACT(dayofweek from event_time) = 4 then 'Wednesday'
        when EXTRACT(dayofweek from event_time) = 5 then 'Thursday'
        when EXTRACT(dayofweek from event_time) = 6 then 'Friday'
        when EXTRACT(dayofweek from event_time) = 7 then 'Saturday'
    end as written_day_of_week,
    avg(event_total_price) as avg_sales_price
from {{ ref('nft_sales_cleaned')}}
group by 1,2
order by 3
