{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        REWARDSRECEIPTITEMLIST
    FROM {{ source("fetch_rewards","RECEIPTS") }}
),

expanded_items AS (
    SELECT
        PARSE_JSON(value):barcode::STRING AS barcode,
        PARSE_JSON(value):description::STRING AS description,
        PARSE_JSON(value):finalPrice::FLOAT AS final_price,
        PARSE_JSON(value):itemPrice::FLOAT AS item_price,
        PARSE_JSON(value):needsFetchReview::BOOLEAN AS needs_fetch_review,
        PARSE_JSON(value):partnerItemId::STRING AS partner_item_id,
        PARSE_JSON(value):preventTargetGapPoints::BOOLEAN AS prevent_target_gap_points,
        PARSE_JSON(value):quantityPurchased::INT AS quantity_purchased,
        PARSE_JSON(value):userFlaggedBarcode::STRING AS user_flagged_barcode,
        PARSE_JSON(value):userFlaggedNewItem::BOOLEAN AS user_flagged_new_item,
        PARSE_JSON(value):userFlaggedPrice::FLOAT AS user_flagged_price,
        PARSE_JSON(value):userFlaggedQuantity::INT AS user_flagged_quantity
    FROM source_data,
    LATERAL FLATTEN(input => PARSE_JSON(REWARDSRECEIPTITEMLIST))
)

SELECT
    barcode,
    description,
    final_price,
    item_price,
    needs_fetch_review,
    partner_item_id,
    prevent_target_gap_points,
    quantity_purchased,
    user_flagged_barcode,
    user_flagged_new_item,
    user_flagged_price,
    user_flagged_quantity
FROM expanded_items
