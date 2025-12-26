
SELECT
    "data":request_id::varchar(100) AS request_id,
    "data":"customeR iD"::varchar(50) AS customer_id,
    "data":"agent ID"::int AS agent_id,
    "data":"COMPLAINT_catego ry"::varchar(100) AS complaint_category,
    "data":resolutionstatus::varchar(50) AS resolution_status,
    "data":request_date::timestamp AS request_date,
    -- TRY CATCH: If resolution_date is "", TRY_TO_TIMESTAMP returns NULL instead of crashing
    TRY_TO_TIMESTAMP("data":resolution_date::varchar) AS resolution_date,
    DATEDIFF(hour, "data":request_date::timestamp, TRY_TO_TIMESTAMP("data":resolution_date::varchar)) AS resolution_hours,
    "data":webFormGenerationDate::DATE AS form_generation_date,
    "data":_source_table::varchar(100) AS source_postgres_table,
    TO_TIMESTAMP_NTZ("data":"_data_load_time"::bigint, 6) AS source_extracted_at,
    "snowflake_load_time" AS snowflake_loaded_at

FROM {{ source('raw_data', 'WEBSITE_FORMS') }} ;
