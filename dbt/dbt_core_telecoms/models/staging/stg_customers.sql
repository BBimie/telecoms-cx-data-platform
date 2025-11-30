WITH source AS (
    SELECT * FROM {{ source('raw_data', 'CUSTOMERS') }}
),

renamed AS (
    SELECT
        data:customer_id::varchar(50) AS customer_id,
        data:name::varchar(100) AS customer_name,
        data:email::varchar(100) AS email,
        data:address::varchar(255) AS address,
        data:Gender::varchar(10) AS gender,
        data:"DATE of biRTH"::date AS date_of_birth,
        data:signup_date::date AS signup_date,
        TO_TIMESTAMP_NTZ(data:"_data_load_time"::bigint, 6) AS source_extracted_at,
        snowflake_load_time AS snowflake_loaded_at
    FROM source
)

SELECT * FROM renamed
