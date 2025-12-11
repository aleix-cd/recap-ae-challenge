with active_rejected_user_invoices as (
  select distinct invoice_id
  from {{ ref('stg_recap_app__invoice_transaction_matches') }}
  qualify row_number() over (
    partition by invoice_id, transaction_id
    order by created_at desc
  ) = 1
  where expert_type = 'user'
    and expert_opinion = 'rejected'
)

select
  sum(
    case
      when invoices.currency = 'EUR'
        then invoices.invoice_total_amount
      when exchange_rates.exchange_rate is not null
        then invoices.invoice_total_amount * exchange_rates.exchange_rate
      else 0
    end
  ) as total_monetary_amount_active_rejected_in_eur
from {{ ref('stg_recap_app__invoices') }} as invoices
inner join active_rejected_user_invoices
  on invoices.invoice_id = active_rejected_user_invoices.invoice_id
left join {{ ref('exchange_rates') }} as exchange_rates
  on invoices.currency = exchange_rates.from_currency
    and invoices.invoice_date = exchange_rates.calendar_date;