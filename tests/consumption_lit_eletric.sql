WITH electric_vehicles AS (
    SELECT
        suk_vehicle_key
    FROM {{ ref('dim_vehicle') }}
    WHERE fuel_type = 'Elétrico'
      AND meta_is_current = TRUE
),

invalid_expenses AS (
    SELECT
        d.suk_vehicle_key,
        d.nm_fuel_l
    FROM {{ ref('fact_vehicle_daily_snapshot') }} d
    INNER JOIN electric_vehicles ev
        ON d.suk_vehicle_key = ev.suk_vehicle_key
    WHERE d.nm_fuel_l IS NOT NULL
      AND d.nm_fuel_l > 0
)

SELECT *
FROM invalid_expenses
