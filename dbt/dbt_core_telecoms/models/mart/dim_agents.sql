WITH agents AS (
    SELECT * FROM {{ ref('stg_agents') }}
)

SELECT
    agent_id,
    agent_name,
    experience_level,
    state AS agent_location,
    source_extracted_at,
    snowflake_loaded_at
FROM agents