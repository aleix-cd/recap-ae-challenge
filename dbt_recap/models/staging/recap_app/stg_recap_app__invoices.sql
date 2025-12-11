with source as (
  select * from {{ source('recap_app', 'invoices') }}
),

renamed as (
  select
    invoice_id,
    company_id,

    company_name,
    counter_party,
    currency,
    invoice_number,
    totalAmount as invoice_total_amount,

    created_at,
    invoice_date,
    invoice_due_date
  from source
)

select * from renamed