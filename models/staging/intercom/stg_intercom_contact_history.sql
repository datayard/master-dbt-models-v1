SELECT
    contact.id as contactId,
    contact.updated_at as updatedAt,
    contact.email as email,
    contact.created_at as createdAt,
    contact.custom_vyuser_id as vyUserID,
    contact.last_seen_at as lastSeenAt
FROM
    {{ source('intercom', 'contact_history') }} as contact
WHERE
    true