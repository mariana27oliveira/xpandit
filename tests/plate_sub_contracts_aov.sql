WITH plates_sub AS (
    SELECT
        COUNT(*) AS num_plates_sub
    FROM {{ ref('dim_fleet_contract') }} c
    WHERE c.meta_is_current = TRUE
      AND c.contract_status = 'Activo'
      AND (
            c.contract_end_date IS NULL
            OR year(c.contract_end_date) = YEAR(TO_TIMESTAMP('{{ var("exec_date") }}'))
          )
),

contracts_aov AS (
    SELECT
        COUNT(*) AS nm_contracts_aov
    FROM {{ ref('dim_fleet_contract') }} c
    WHERE c.meta_is_current = TRUE
      AND c.contract_status = 'Activo'
      AND c.contract_type = 'Alug. Operacional'
      AND (
            c.contract_end_date IS NULL
            OR year(c.contract_end_date) = YEAR(TO_TIMESTAMP('{{ var("exec_date") }}'))
          )
)

SELECT
    v.num_plates_sub,
    a.nm_contracts_aov
FROM plates_sub v
CROSS JOIN contracts_aov a
WHERE v.num_plates_sub > a.nm_contracts_aov
