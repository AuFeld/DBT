-- incremental models require configuration
{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}

WITH src_reviews aS (
    SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null

-- how to increment the table with jinja IF statement
-- {{ this }} refers to fct_reviews.sql (this) file
{% if is_incremental() %}
    AND review_date > (select max(review_date) from {{ this }})
{% endif %}