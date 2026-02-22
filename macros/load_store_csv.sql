{% macro load_store_csv() %}

COPY INTO BRONZE.STORES_RAW
FROM (
    SELECT 
        $1 AS STORE_ID,
        $2 AS TYPE,
        $3 AS SIZE,
        CURRENT_TIMESTAMP() AS LOAD_DTS
    FROM @BRONZE.WALMART_STAGE/stores.csv
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
FORCE = TRUE;

{% endmacro %}