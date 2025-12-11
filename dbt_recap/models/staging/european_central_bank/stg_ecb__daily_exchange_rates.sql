with seed as (
  select * from {{ ref('daily_exchange_rates') }}
),

renamed as (
  select
    id as exchange_rate_id,
    to_currency,
    from_currency,
    exchange_rate,
    provider as exchange_rate_provider,
    valid_from
  from seed
)

select * from renamed