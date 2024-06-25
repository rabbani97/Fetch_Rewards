
{{ config(materialized='table') }}
WITH source_data AS (
    SELECT
        _ID,
        BARCODE,
        CATEGORY,
        BRANDCODE,
        CATEGORYCODE,
        CPG,
        NAME,
        TOPBRAND,
        CPG AS json_cpg  -- Assuming 'CPG' contains JSON data
    FROM {{ source('fetch_rewards', 'BRANDS') }}
)

, parsed_data AS (
    SELECT
        -- Use macro to parse '_ID'
        {{ parse_json_field('_ID', '"$oid"') }} AS id_oid,
        BARCODE,
        CATEGORY,
        BRANDCODE,
        CATEGORYCODE,
        NAME,
        TOPBRAND,
        -- Use macro for 'CPG'
        {{ parse_json_field('json_cpg', '"$id"."$oid"') }} AS cpg_id_oid,
        {{ parse_json_field('json_cpg', '"$ref"') }} AS cpg_ref
    FROM source_data
)

SELECT
    id_oid,
    BARCODE AS barcode,
    CATEGORY AS category,
    BRANDCODE AS brandcode,
    CATEGORYCODE AS categorycode,
    NAME AS name,
    TOPBRAND AS topbrand,
    cpg_id_oid,
    cpg_ref
FROM parsed_data

