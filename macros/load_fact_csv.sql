{% macro load_fact_csv() %}

COPY INTO BRONZE.FACTS_RAW 
FROM (
    SELECT
        $1 AS STORE_ID,
        $2 AS DATE,
        $3 AS TEMPERATURE,
        $4 AS FUEL_PRICE,
        $5 AS MARKDOWN_1,
        $6 AS MARKDOWN_2,
        $7 AS MARKDOWN_3,
        $8 AS MARKDOWN_4,
        $9 AS MARKDOWN_5,
        $10 AS CPI,
        $11 AS UNEMPLOYMENT,
        $12 AS IS_HOLIDAY,
        CURRENT_TIMESTAMP() AS LOAD_DTS
    FROM @BRONZE.WALMART_STAGE/fact.csv
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NA'))
FORCE = TRUE;

{% endmacro %}