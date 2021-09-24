SELECT
    freeDomains.domain as emailDomain
FROM
    --ops_utility_tables.free_domain_list
    {{ source('ops_utility_tables','free_domain_list') }} as freeDomains
WHERE
    true