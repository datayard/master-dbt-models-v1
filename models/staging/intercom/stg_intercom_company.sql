SELECT
    LEFT (company_history.company_id,24) as companyId,
    company_history.updated_at as updatedDate,
    company_history.created_at as createdDate,
    company_history.name as companyName,
    company_history.session_count as sessionsCount,
    company_history.user_count as userCount,
    company_history.website as website
FROM
    {{ source('intercom', 'company_history') }} as company_history
WHERE
    true