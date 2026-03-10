WITH consumption_vehicle AS (
    SELECT
        f.suk_vehicle_key,
        v.license_plate,
        v.buk_segment_key,
        AVG(f.nm_fuel_l / NULLIF(f.nm_actual_km, 0)) AS av_consumption_lit_km
    FROM {{ ref('fact_vehicle_daily_snapshot') }} f
    LEFT JOIN {{ ref('dim_vehicle') }} v
        ON f.suk_vehicle_key = v.suk_vehicle_key
    WHERE f.flg_is_active_vehicle = 1 AND v.meta_is_current = TRUE
    GROUP BY
        f.suk_vehicle_key,
        v.license_plate,
        v.buk_segment_key
),

av_segment AS (
    SELECT
        buk_segment_key,
        STDDEV(av_consumption_lit_km) AS desvio_padrao_segment
    FROM consumption_vehicle
    WHERE av_consumption_lit_km IS NOT NULL
    GROUP BY buk_segment_key
),

outliers AS (
    SELECT
        c.suk_vehicle_key,
        c.license_plate,
        c.buk_segment_key,
        c.av_consumption_lit_km,
        d.desvio_padrao_segment,
        3 * d.desvio_padrao_segment AS limite_superior
    FROM consumption_vehicle c
    INNER JOIN av_segment d
        ON c.buk_segment_key = d.buk_segment_key
    WHERE
        d.desvio_padrao_segment IS NOT NULL
        AND c.av_consumption_lit_km > 3 * d.desvio_padrao_segment
)

SELECT *
FROM outliers
