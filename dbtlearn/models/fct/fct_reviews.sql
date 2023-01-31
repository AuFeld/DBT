-- incremental models require configuration
{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}
WITH src_reviews AS (
    SELECT * FROM {{ ref('src_reviews') }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_id,
    * 
FROM src_reviews
WHERE review_text is not null

-- increment table with jinja IF statement
-- {{ this }} refers to fct_reviews.sql (this) file
{% if is_incremental() %}
    AND review_date > (select max(review_date) from {{ this }})
{% endif %}