
SELECT
    "data":complaint_id::varchar(100) AS complaint_id,
    "data":"customeR iD"::varchar(50) AS customer_id,
    "data":"agent ID"::int AS agent_id,
    "data":"COMPLAINT_catego ry"::varchar(100) AS complaint_category,
    "data":media_channel::varchar(50) AS media_channel,
    "data":resolutionstatus::varchar(50) AS resolution_status,
    "data":request_date::timestamp AS request_date,
    TRY_TO_TIMESTAMP("data":resolution_date::varchar) AS resolution_date,
    DATEDIFF(hour, "data":request_date::timestamp, TRY_TO_TIMESTAMP("data":resolution_date::varchar)) AS resolution_hours,
    "data":MediaComplaintGenerationDate::date AS complaint_generation_date,
    "data":_source_file::varchar(100) AS source_filename,
    TO_TIMESTAMP_NTZ("data":"_data_load_time"::bigint, 6) AS source_extracted_at,
    "snowflake_load_time" AS snowflake_loaded_at

FROM {{ source('raw_data', 'SOCIAL_MEDIA') }} ;
