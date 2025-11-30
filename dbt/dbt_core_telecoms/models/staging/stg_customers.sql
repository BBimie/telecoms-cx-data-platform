WITH source AS (
    SELECT * FROM {{ source('raw_data', 'CUSTOMERS') }}
),

renamed AS (
    SELECT
        "data":customer_id::varchar(50) AS customer_id,
        "data":name::varchar(100) AS customer_name,

        -- CLEAN EMAIL FIELD
        CASE 
            -- Fix Gmail typos gmail.om > gmail.com
            WHEN LOWER("data":email::varchar(100)) LIKE '%@gmail.om' 
                THEN REGEXP_REPLACE(LOWER("data":email::varchar(100)), '@gmail.om$', '@gmail.com')
            
            -- Fix Hotmail typos hotmai.com > hotmail.com
            WHEN LOWER("data":email::varchar(100)) LIKE '%@hotmai.com' 
                THEN REGEXP_REPLACE(LOWER("data":email::varchar(100)), '@hotmai.com$', '@hotmail.com')
            
            -- Catch-all for other ".om" endings
            WHEN LOWER("data":email::varchar(100)) LIKE '%.om' 
                THEN REGEXP_REPLACE(LOWER("data":email::varchar(100)), '\.om$', '.com')

            ELSE LOWER("data":email::varchar(100))
        END AS email,
        "data":address::varchar(255) AS address,
        "data":Gender::varchar(10) AS gender,
        "data":"DATE of biRTH"::date AS date_of_birth,
        "data":signup_date::date AS signup_date,
        TO_TIMESTAMP_NTZ("data":"_data_load_time"::bigint, 6) AS source_extracted_at,
        "snowflake_load_time" AS snowflake_loaded_at
    FROM source
)

SELECT * FROM renamed
