{{ config(materialized='incremental', unique_key='transaction_id') }}

SELECT
    t.transaction_id,
    t.account_id,
    t.related_account_id,
    t.txn_type,
    t.direction,
    t.amount,
    t.balance_after,
    t.txn_status,
    t.reference,
    t.txn_desc,
    t.txn_time,
    CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_transactions')  }} AS t
    LEFT JOIN {{ ref('stg_accounts') }} AS a
        ON t.account_id = a.account_id

{% if is_incremental() %}
  WHERE t.txn_time > (SELECT MAX(txn_time) FROM {{ this }})
{% endif %}


