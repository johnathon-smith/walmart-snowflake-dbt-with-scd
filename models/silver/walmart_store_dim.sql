{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_id = ['store_id','dept_id'],
        merge_exclude_columns = ['insert_date'],
        schema = 'SILVER'
    )
}}

WITH DEPARTMENTS AS (
    SELECT DISTINCT
        STORE_ID,
        DEPT_ID
    FROM {{source('RAW','DEPARTMENTS_RAW')}}
    WHERE LOAD_DTS = (SELECT MAX(LOAD_DTS) FROM {{source('RAW', 'DEPARTMENTS_RAW')}} )
),

STORES AS (
    SELECT DISTINCT
        STORE_ID,
        TYPE,
        SIZE
    FROM {{ source( 'RAW', 'STORES_RAW' ) }}
    WHERE LOAD_DTS = (SELECT MAX(LOAD_DTS) FROM {{ source('RAW', 'STORES_RAW' ) }} )
),

WALMART_STORE_MERGE AS (
    SELECT
        D.STORE_ID,
        D.DEPT_ID,
        S.TYPE AS STORE_TYPE,
        S.SIZE AS STORE_SIZE,
        CURRENT_TIMESTAMP() AS INSERT_DATE,
        CURRENT_TIMESTAMP() AS UPDATE_DATE
    FROM DEPARTMENTS D
    JOIN STORES S
        ON D.STORE_ID = S.STORE_ID
)

SELECT * FROM WALMART_STORE_MERGE