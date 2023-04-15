select
  id,
  collection_slug,
  collection_name,
  collection_url,
  asset_id,
  asset_contract_date,
  asset_url,
  asset_img_url,
  event_id,
  event_time,
  event_auction_type,
  event_contract_address,
  event_payment_symbol,
  event_quantity * 1000 as event_quantity, --I had to divide by 1000 in seed file to upload
  event_total_price,

--New Reusable Fields

  EXTRACT(dayofweek from event_time) as number_day_of_week_event,
  case
      when EXTRACT(dayofweek from event_time) = 1 then 'Sunday'
      when EXTRACT(dayofweek from event_time) = 2 then 'Monday'
      when EXTRACT(dayofweek from event_time) = 3 then 'Tuesday'
      when EXTRACT(dayofweek from event_time) = 4 then 'Wednesday'
      when EXTRACT(dayofweek from event_time) = 5 then 'Thursday'
      when EXTRACT(dayofweek from event_time) = 6 then 'Friday'
      when EXTRACT(dayofweek from event_time) = 7 then 'Saturday'
  end as written_day_of_week_event,
  event_total_price *1.0/(event_quantity * 1000) as price_per_item


from {{ ref('nft_sales_cleaned')}}
