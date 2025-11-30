WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    customer_name,
    gender,
    email,
    address,
    date_of_birth,
    signup_date,
    -- Business Logic: 'Tenure' (How many days have they been a customer?)
    DATEDIFF(day, signup_date, CURRENT_DATE()) AS tenure_days,
    source_extracted_at,
    snowflake_loaded_at
FROM customers