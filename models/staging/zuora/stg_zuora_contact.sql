SELECT
        contact.id as contactId,
        contact.account_id as accountId,
        contact.created_by_id as createdById,
        contact.created_date as createdDate,
        contact.city as city,
        contact.country as country,
        contact.first_name as firstName,
        contact.last_name as lastName,
        contact.personal_email as personalEmail,
        contact.postal_code as postalCode,
        contact.state as state,
        contact.work_email as workEmail,
        contact.updated_by_id as updatedById,
        contact.updated_date as updatedDate,
        contact._fivetran_deleted as fivetranDeleted,
        contact._fivetran_synced as fivetranSynced
FROM
        {{ source ('zuora' , 'contact')}} as contact

WHERE

        TRUE
