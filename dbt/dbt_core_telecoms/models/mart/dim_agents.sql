
SELECT
    agent_id,
    agent_name,
    experience,
    state AS agent_location,
    source_extracted_at,
    snowflake_loaded_at
FROM {{ ref('stg_agents') }} ;