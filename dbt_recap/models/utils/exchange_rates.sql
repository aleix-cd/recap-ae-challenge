with calendar as (
  select * from {{ ref('calendar') }}
),

stg_filtered as (
  select *
  from {{ ref('stg_ecb__daily_exchange_rates') }}
  where exchange_rate_provider = 'European Central Bank'
    and from_currency in (
      'GBP',
      'USD'
    )
    and to_currency = 'EUR'
),

get_next_valid_from as (
  select
    *,
    /* get next valid_from date for this currency pair */
    lead(valid_from) over (
      partition by to_currency, from_currency
      order by valid_from asc
    ) as next_date_tmp,
    /*  current_date+10 years as we join on the conditions of
        date >= valid_from and < valid_to for conversions
    */
    coalesce(
      lead(valid_from) over (
        partition by to_currency, from_currency
        order by valid_from asc
      ),
      date_add(current_date(), interval 10 year)
    ) as valid_to
  from stg_filtered
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(
      [
        'calendar.calendar_date',
        'get_next_valid_from.to_currency',
        'get_next_valid_from.from_currency']
      )
    }} as pk,
    calendar.calendar_date,
    get_next_valid_from.to_currency,
    get_next_valid_from.from_currency,
    get_next_valid_from.exchange_rate,
    get_next_valid_from.valid_from,
    get_next_valid_from.valid_to
  from calendar
  left join get_next_valid_from
    on calendar.calendar_date >= get_next_valid_from.valid_from
      and calendar.calendar_date < get_next_valid_from.valid_to
  where get_next_valid_from.exchange_rate is not null
)

select * from final
