with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('1900-01-01' as date)",
      end_date="date_add(today(), interval 5 year)"
    )
  }}
),

date_formatted as (
  select date(date_day) as date_day
  from date_spine
),

final as (
  select
    date_day as calendar_date,

    -- Year components
    extract(year from date_day) as year_actual,
    date_part('dayofyear', date_day) as day_of_year,
    date_trunc('year', date_day) as first_day_of_year,

    -- Quarter components
    extract(quarter from date_day) as quarter_actual,
    concat(
      cast(extract(year from date_day) as varchar),
      '-q',
      cast(extract(quarter from date_day) as varchar)
    ) as year_quarter,
    row_number() over (
      partition by extract(year from date_day), extract(quarter from date_day)
      order by date_day
    ) as day_of_quarter,
    date_trunc('quarter', date_day) as first_day_of_quarter,

    -- Month components
    strftime(date_day, '%b') as month_name, -- DuckDB strftime
    extract(month from date_day) as month_actual,
    extract(day from date_day) as day_of_month,
    date_trunc('month', date_day) as first_day_of_month,
    last_day(date_day) as last_day_of_month,

    -- Week components
    date_trunc('week', date_day) as first_day_of_week,
    cast(strftime(date_day, '%u') as integer) as day_of_week,
    date_trunc('week', date_day) + interval 6 day as last_day_of_week,
    strftime(date_day, '%a') as day_name,

    -- Current Period Flags
    coalesce(
      date_day <= current_date()
      and extract(month from date_day) = extract(month from current_date())
      and extract(year from date_day) = extract(year from current_date()),
      false
    ) as in_mtd_current_year,

    coalesce(
      date_day <= current_date()
      and extract(quarter from date_day) = extract(quarter from current_date())
      and extract(year from date_day) = extract(year from current_date()),
      false
    ) as in_qtd_current_year,

    coalesce(
      date_day <= current_date()
      and extract(year from date_day) = extract(year from current_date()),
      false
    ) as in_ytd_current_year,

    -- Last Year Flags
    current_date - interval 1 year as today_last_year, -- DuckDB interval arithmetic

    coalesce(
      date_day <= current_date - interval 1 year
      and extract(month from date_day) = extract(month from current_date - interval 1 year)
      and extract(year from date_day) = extract(year from current_date - interval 1 year),
      false
    ) as in_mtd_last_year,

    coalesce(
      date_day <= current_date - interval 1 year
      and extract(quarter from date_day) = extract(quarter from current_date - interval 1 year)
      and extract(year from date_day) = extract(year from current_date - interval 1 year),
      false
    ) as in_qtd_last_year,

    coalesce(
      date_day <= current_date - interval 1 year
      and extract(year from date_day) = extract(year from current_date - interval 1 year),
      false
    ) as in_ytd_last_year,

    -- Last 2 Year Flags
    current_date - interval 2 year as today_last_2_year, -- DuckDB interval arithmetic

    coalesce(
      date_day <= current_date - interval 2 year
      and extract(year from date_day) = extract(year from current_date - interval 2 year)
      and extract(month from date_day) = extract(month from current_date - interval 2 year),
      false
    ) as in_mtd_last_2_year,

    coalesce(
      date_day <= current_date - interval 2 year
      and extract(quarter from date_day) = extract(quarter from current_date - interval 2 year)
      and extract(year from date_day) = extract(year from current_date - interval 2 year),
      false
    ) as in_qtd_last_2_year,

    coalesce(
      date_day <= current_date - interval 2 year
      and extract(year from date_day) = extract(year from current_date - interval 2 year),
      false
    ) as in_ytd_last_2_year

  from date_formatted -- Corrected CTE reference
)

select * from final