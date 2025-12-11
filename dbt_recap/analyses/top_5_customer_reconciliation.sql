with active_approved_matches as (
  select
    matches.invoice_id,
    matches.expert_type,
    matches.created_at,
    invoices.company_name
  from {{ ref('stg_recap_app__invoice_transaction_matches') }} as matches
  inner join {{ ref('stg_recap_app__invoices') }} as invoices
    on matches.invoice_id = invoices.invoice_id
  where matches.expert_opinion = 'approved'
  qualify row_number() over (
    partition by matches.invoice_id, matches.transaction_id
    order by matches.created_at desc
  ) = 1
),

unique_reconciled_invoices as (
  select
    invoice_id,
    company_name,
    expert_type
  from active_approved_matches
  qualify row_number() over (
    partition by invoice_id
    order by created_at desc
  ) = 1
),

company_reconciliation_summary as (
  select
    company_name,
    sum(case when expert_type = 'system' then 1 else 0 end) as system_reconciled_count,
    sum(case when expert_type = 'user' then 1 else 0 end) as user_reconciled_count,
    count(invoice_id) as total_reconciled_invoices
  from unique_reconciled_invoices
  group by company_name
)

select
  company_name,
  total_reconciled_invoices,
  (system_reconciled_count * 100.0) / total_reconciled_invoices
    as percentage_reconciled_by_system,
  (user_reconciled_count * 100.0) / total_reconciled_invoices
    as percentage_reconciled_by_user
from company_reconciliation_summary
order by total_reconciled_invoices desc
limit 5;