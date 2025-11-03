{% snapshot accounts_snapshot %}

{{
    config(
        target_schema='MARTS',
        unique_key='account_id',
        strategy='check',
        check_cols=['customer_id', 
                    'account_type', 
                    'status', 
                    'balance', 
                    'interest_rate', 
                    ]
    )
}}
SELECT * FROM  {{ ref('stg_accounts') }}

{% endsnapshot %}