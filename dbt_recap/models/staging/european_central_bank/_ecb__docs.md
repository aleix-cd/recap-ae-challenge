{% docs exchange_rate_id %}
The Primary Key of the Exchange Rates source table.
{% enddocs %}

{% docs to_currency %}
The target currency (the one we want to convert to).
{% enddocs %}

{% docs from_currency %}
The source currency (the one we want to convert from).
{% enddocs %}

{% docs exchange_rate %}
The exchange rate of a pair of currencies at the end of a specific date.
{% enddocs %}

{% docs exchange_rate_provider %}
The name of the exchange rate provider. Currently we just have one, the European Central Bank.
{% enddocs %}

{% docs valid_from %}
The date from which this record is considered active.
{% enddocs %}