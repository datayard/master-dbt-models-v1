SELECT
    kpis.target_period as targetPeriod,
    kpis.kpi as kpi,
    kpis.time_period as timePeriod,
    kpis.target as target
FROM
    {{ source('ops_utility_tables','kpis') }} as kpis
WHERE
    true