WITH electric_vehicles AS (
    SELECT
        suk_vehicle_key
    FROM {{ ref('dim_vehicle') }}
    WHERE fuel_type = 'Elétrico' AND meta_is_current = TRUE
),

invalid_contracts AS (
    SELECT
        c.suk_vehicle_key,
        c.km_limit
    FROM {{ ref('dim_fleet_contract') }} c
    INNER JOIN electric_vehicles v
        ON c.suk_vehicle_key = v.suk_vehicle_key
    WHERE c.contract_type = 'Alug. Operacional'
      AND c.km_limit > 180000 AND c.meta_is_current = TRUE
)

SELECT *
FROM invalid_contracts
