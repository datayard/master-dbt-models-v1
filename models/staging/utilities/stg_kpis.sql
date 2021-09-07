SELECT
    kpis.quarter as quarter,
    kpis.fiscal_year as fiscalYear,
    kpis.period as targetPeriod,
    kpis.kpi_category as kpiCategory,
    kpis.kpi as kpi,
    kpis.description as description,
    kpis.start_date as startDate,
    kpis.target as target
FROM
    {{ source('ops_utility_tables','kpis') }} as kpis
WHERE
    true