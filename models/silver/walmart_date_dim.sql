{{
    config (
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'DATE_ID',
        merge_exclude_columns = ['INSERT_DATE'],
        schema = 'SILVER'
    )
}}

WITH DISTINCT_DATES AS (
    SELECT DISTINCT
        DATE AS STORE_DATE,
        IS_HOLIDAY
    FROM {{ source('RAW','DEPARTMENTS_RAW') }}
    WHERE LOAD_DTS = (SELECT MAX(LOAD_DTS) FROM {{ source('RAW','DEPARTMENTS_RAW') }} )
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['STORE_DATE']) }} AS DATE_ID,
    STORE_DATE,
    IS_HOLIDAY,
    CURRENT_TIMESTAMP() AS INSERT_DATE,
    CURRENT_TIMESTAMP() AS UPDATE_DATE
FROM DISTINCT_DATES