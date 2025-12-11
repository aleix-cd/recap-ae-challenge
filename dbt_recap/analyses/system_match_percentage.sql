with invoice_system_match_flag as (
  select distinct
    invoices.invoice_id,
    case
      when system_matches.invoice_id is not null then 1
      else 0
    end as has_system_match
  from {{ ref('stg_recap_app__invoices') }} as invoices
  left join (
    select distinct invoice_id
    from {{ ref('stg_recap_app__invoice_transaction_matches') }}
    where expert_type = 'system'
  ) as system_matches
    on invoices.invoice_id = system_matches.invoice_id
)

select
  (sum(has_system_match) * 100.0) / count(invoice_id)
    as percentage_invoices_with_system_match
from invoice_system_match_flag;