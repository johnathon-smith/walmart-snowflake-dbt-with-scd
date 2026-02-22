{% snapshot walmart_fact_snapshot %}

{{
    config(
        alias = 'WALMART_FACT_TABLE',
        strategy = 'check',
        unique_key = ['STORE_ID','DEPT_ID','DATE_ID'],
        check_cols = [
            'STORE_SIZE',
            'STORE_WEEKLY_SALES',
            'FUEL_PRICE',
            'STORE_TEMPERATURE',
            'UNEMPLOYMENT',
            'CPI',
            'MARKDOWN_1',
            'MARKDOWN_2',
            'MARKDOWN_3',
            'MARKDOWN_4',
            'MARKDOWN_5'
        ],
        schema = 'SILVER'
    )
}}

WITH STORE_SALES AS (
    SELECT
        STORE_ID,
        DEPT_ID,
        DATE,
        WEEKLY_SALES,
    FROM {{ source('RAW','DEPARTMENTS_RAW') }}
    WHERE LOAD_DTS = (SELECT MAX(LOAD_DTS) FROM {{ source('RAW','DEPARTMENTS_RAW') }} )
),

STORE_SIZE AS (
    SELECT DISTINCT
        STORE_ID,
        STORE_SIZE
    FROM {{ ref("walmart_store_dim") }}
),

DATE_DIM AS (
    SELECT
        DATE_ID,
        STORE_DATE
    FROM {{ ref("walmart_date_dim") }}
),

STORE_FACTS AS (
    SELECT
        STORE_ID,
        DATE,
        FUEL_PRICE,
        TEMPERATURE,
        UNEMPLOYMENT,
        CPI,
        MARKDOWN_1,
        MARKDOWN_2,
        MARKDOWN_3,
        MARKDOWN_4,
        MARKDOWN_5
    FROM {{ source('RAW','FACTS_RAW') }}
    WHERE LOAD_DTS = (SELECT MAX(LOAD_DTS) FROM {{ source('RAW','FACTS_RAW') }} )
),

WALMART_FACT_TABLE AS (
    SELECT
        SALES.STORE_ID,
        SALES.DEPT_ID,
        DATES.DATE_ID,
        SIZES.STORE_SIZE,
        SALES.WEEKLY_SALES AS STORE_WEEKLY_SALES,
        FACTS.FUEL_PRICE,
        FACTS.TEMPERATURE AS STORE_TEMPERATURE,
        FACTS.UNEMPLOYMENT,
        FACTS.CPI,
        FACTS.MARKDOWN_1,
        FACTS.MARKDOWN_2,
        FACTS.MARKDOWN_3,
        FACTS.MARKDOWN_4,
        FACTS.MARKDOWN_5
    FROM STORE_FACTS FACTS
    JOIN STORE_SALES SALES
        ON FACTS.STORE_ID = SALES.STORE_ID
        AND FACTS.DATE = SALES.DATE
    JOIN STORE_SIZE SIZES
        ON FACTS.STORE_ID = SIZES.STORE_ID
    JOIN DATE_DIM DATES
        ON FACTS.DATE = DATES.STORE_DATE
)

SELECT * FROM WALMART_FACT_TABLE

{% endsnapshot %}