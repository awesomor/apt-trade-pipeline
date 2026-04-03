WITH source as (
    SELECT * FROM {{ source('apt_trade_raw', 'APT_TRADE_RAW') }}
),
cleaned as (
    SELECT 
        TRIM(LAWD_CD) as LAWD_CD,
        TRIM(DEAL_YMD) as DEAL_YMD,
        TRIM(APT_NM) as APT_NM,
        10000 * CAST(REPLACE(DEAL_AMOUNT, ',', '') as INT) as DEAL_AMOUNT,
        TO_DATE(DEAL_YEAR || '-' || LPAD(DEAL_MONTH, 2, '0') || '-' || LPAD(DEAL_DAY, 2, '0'), 'YYYY-MM-DD') as DEAL_DATE,
        CAST("FLOOR" as INT) as "FLOOR",
        CAST(BUILD_YEAR as INT) as BUILD_YEAR,
        TRIM(JIBUN) as JIBUN
    FROM source
)

SELECT * FROM cleaned
