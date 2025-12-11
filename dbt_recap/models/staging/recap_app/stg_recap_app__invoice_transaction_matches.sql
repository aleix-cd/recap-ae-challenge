with source as (
  select * from {{ source('recap_app', 'invoice_transaction_matches')}}
),

renamed as (
  select
    {{ dbt_utils.generate_surrogate_key(
      ['invoice_id', 'transaction_id', 'created_at']
      )
    }} as pk,
    invoice_id,
    transaction_id,

    expert_email,
    expert_opinion,
    expert_type,

    created_at
  from source
)

select *from renamed