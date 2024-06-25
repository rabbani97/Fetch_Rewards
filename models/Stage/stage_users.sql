
{{ config(materialized='table') }}
WITH source_data AS (
    SELECT
        
        _ID,
        ACTIVE,
        CREATEDDATE,
        LASTLOGIN,
        ROLE,
        SIGNUPSOURCE,
        STATE
    FROM {{source("fetch_rewards","USERS")}}  -- Adjust 'your_source_name' and 'users' as necessary
)

, parsed_data AS (
    SELECT
                -- Parsing the JSON in _ID to extract the '$oid' value
        TRY_PARSE_JSON(_ID):"$oid"::STRING AS id,
        ACTIVE,
        -- Converting CREATEDDATE from JSON-stored UNIX timestamp to a readable datetime
        TO_CHAR(TO_TIMESTAMP_NTZ(TO_NUMBER(TRY_PARSE_JSON(CREATEDDATE):"$date"::STRING)/1000), 'MM-DD-YYYY HH24:MI:SS') AS created_datetime,
        -- Converting LASTLOGIN from JSON-stored UNIX timestamp to formatted datetime
        TO_CHAR(TO_TIMESTAMP_NTZ(TO_NUMBER(TRY_PARSE_JSON(LASTLOGIN):"$date"::STRING)/1000), 'MM-DD-YYYY HH24:MI:SS') AS last_login_datetime,
        ROLE,
        SIGNUPSOURCE,
        STATE
    FROM source_data
)

SELECT
  
    id as USER_ID,
    ACTIVE,
    created_datetime as CREATEDDATE,
    last_login_datetime as LASTLOGIN,
    ROLE,
    SIGNUPSOURCE,
    STATE
FROM parsed_data


