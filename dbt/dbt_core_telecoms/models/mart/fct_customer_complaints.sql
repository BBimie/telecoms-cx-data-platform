WITH 
-- 1. Get the Call Center Data
calls AS (
    SELECT 
        call_id AS interaction_id,
        customer_id,
        agent_id,
        complaint_category,
        resolution_status,
        call_start_time AS interaction_started_at,
        call_end_time AS interaction_ended_at,
        duration_seconds AS resolution_time_sec,
        'Call Center' AS source_channel
    FROM {{ ref('stg_call_logs') }}
),

-- 2. Get the Web Forms Data
web AS (
    SELECT 
        request_id AS interaction_id,
        customer_id,
        agent_id,
        complaint_category,
        resolution_status,
        request_date AS interaction_started_at,
        resolution_date AS interaction_ended_at,
        resolution_hours * 3600 AS resolution_time_sec,
        'Web Form' AS source_channel
    FROM {{ ref('stg_web_forms') }}
),

-- 3. Get the Social Media Data
social AS (
    SELECT 
        complaint_id AS interaction_id,
        customer_id,
        agent_id,
        complaint_category,
        resolution_status,
        request_date AS interaction_started_at,
        resolution_date AS interaction_ended_at
        resolution_hours * 3600 AS resolution_time_sec,
        -- Combine static text with the channel name (e.g. "Social - Twitter")
        'Social - ' || media_channel AS source_channel
    FROM {{ ref('stg_social_media') }}
),

-- 4. Stack them all together
unioned AS (
    SELECT * FROM calls
    UNION ALL
    SELECT * FROM web
    UNION ALL
    SELECT * FROM social
)

-- 5. Final Selection
SELECT 
    interaction_id,
    customer_id,
    agent_id,
    complaint_category,
    resolution_status,
    interaction_started_at,
    interaction_ended_at,
    resolution_time_sec,
    source_channel,
    -- Handy date dimension for filtering by day/month in dashboards
    DATE(interaction_started_at) AS interaction_start_date
    DATE(interaction_started_at) AS interaction_end_date
FROM unioned