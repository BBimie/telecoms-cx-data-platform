WITH source AS (
    SELECT * FROM {{ source('raw_data', 'CALL_CENTER_LOGS') }}
),

renamed AS (
    SELECT
        "data":"call ID"::varchar(50) AS call_id,
        "data":"customeR iD"::varchar(50) AS customer_id,
        "data":"agent ID"::int AS agent_id,
        "data":"COMPLAINT_catego ry"::varchar(100) AS complaint_category,
        "data":resolutionstatus::varchar(50) AS resolution_status,
        "data":call_start_time::timestamp AS call_start_time,
        "data":call_end_time::timestamp AS call_end_time,
        DATEDIFF(second, "data":call_start_time::timestamp, "data":call_end_time::timestamp) AS duration_seconds,
        "data":callLogsGenerationDate::date AS logs_generation_date,
        "data":_source_file::varchar(100) AS source_filename,
        TO_TIMESTAMP_NTZ("data":"_data_load_time"::bigint, 6) AS source_extracted_at,
        "snowflake_load_time" AS snowflake_loaded_at

    FROM source
)

SELECT * FROM renamed
