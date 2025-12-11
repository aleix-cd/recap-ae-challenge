{% docs invoice_id %}
The unique identifier for the invoice. This is the primary key for the `stg_recap_app__invoices` model and is used as a foreign key in related models.
{% enddocs %}

{% docs company_id %}
The unique identifier for the company that issued or received the invoice.
{% enddocs %}

{% docs company_name %}
The name of the company associated with the invoice.
{% enddocs %}

{% docs counter_party %}
The name of the external entity (customer or vendor) involved in the invoice transaction.
{% enddocs %}

{% docs currency %}
The ISO currency code ('EUR', 'GBP', 'USD') in which the invoice total amount is denominated.
{% enddocs %}

{% docs invoice_number %}
The unique reference string assigned to the invoice.
{% enddocs %}

{% docs invoice_total_amount %}
The final, total amount of the invoice, in the specified currency.
{% enddocs %}

{% docs created_at %}
The timestamp when this record was first created into the source system.
{% enddocs %}

{% docs invoice_date %}
The official date the invoice document was issued.
{% enddocs %}

{% docs invoice_due_date %}
The date by which the payment for the invoice is expected.
{% enddocs %}

{% docs pk %}
The primary key of this match record. It is a hash of invoice_id, transaction_id and created_at.
{% enddocs %}

{% docs transaction_id %}
The identifier for the financial transaction linked to the invoice.
{% enddocs %}

{% docs expert_email %}
The email address of the expert who provided the opinion on this match record.
{% enddocs %}

{% docs expert_opinion %}
The expert's review result regarding the invoice/transaction match. Can be either 'approved' or 'rejected'.
{% enddocs %}

{% docs expert_type %}
The category of the expert who provided the opinion. Can be either 'system' (for automatic machine reviews) or
'user' (for human users).
{% enddocs %}