SELECT
    contact_company.company_id as companyId,
    contact_company.contact_id as contactId,
    contact_company.contact_updated_at as contactUpdatedAt
FROM
    {{ source('intercom', 'contact_company_history') }} as contact_company
WHERE
    true