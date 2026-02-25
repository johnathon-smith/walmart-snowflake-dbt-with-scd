{% macro load_department_csv() %}


COPY INTO BRONZE.DEPARTMENTS_RAW 
FROM (
    SELECT
        $1 AS STORE_ID,
        $2 AS DEPT_ID,
        $3 AS DATE,
        $4 AS WEEKLY_SALES,
        $5 AS IS_HOLIDAY,
        CURRENT_TIMESTAMP() AS LOAD_DTS
    FROM @BRONZE.WALMART_STAGE/department.csv
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
FORCE = TRUE;

{% endmacro %}