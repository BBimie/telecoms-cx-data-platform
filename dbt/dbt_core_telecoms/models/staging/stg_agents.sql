WITH source AS (
    SELECT * FROM {{ source('raw_data', 'AGENTS') }}
),

renamed AS (
    SELECT
        data:iD::int AS agent_id,
        data:NamE::varchar(100) AS agent_name,
        data:experience::varchar(50) AS experience,
        data:state::varchar(50) as state,
        TO_TIMESTAMP_NTZ(data:"_data_load_time"::bigint, 6) AS source_extracted_at,
        snowflake_load_time AS snowflake_loaded_at
    FROM source
)

SELECT * FROM renamed
