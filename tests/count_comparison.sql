WITH count_vehicles AS (
    SELECT COUNT(DISTINCT buk_vehicle_code) AS vehicles
    FROM {{ ref('dim_vehicle') }}
    WHERE meta_is_current = TRUE
),

count_contracts AS (
    SELECT COUNT(DISTINCT buk_contract_code) AS contracts
    FROM {{ ref('dim_fleet_contract') }}
    WHERE meta_is_current = TRUE
)

SELECT
    vehicles,
    contracts
FROM count_vehicles
CROSS JOIN count_contracts
WHERE vehicles <= contracts
