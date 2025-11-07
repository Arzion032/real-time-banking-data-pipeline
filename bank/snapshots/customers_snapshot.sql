{% snapshot customers_snapshot %}

{{
    config(
        target_schema='MARTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 
                    'last_name', 
                    'email', 
                    'phone', 
                    'cust_address',
                    'cust_status',]
    )
}}
SELECT * FROM  {{ ref('stg_customers') }}

{% endsnapshot %}