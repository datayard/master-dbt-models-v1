SELECT
    contact.id as contactId,
    contact.updated_at as updatedDate,
    contact.email as email,
    contact.created_at as createdDate,
    contact.custom_vyuser_id as vidyardUserID,
    contact.last_seen_at as lastSeenDate,
    contact.custom_product_page as customProductPage,
    contact.custom_location as customLocation,
    contact.custom_upgrade_paths_experience as customUpgradePathsExperience
FROM
    {{ source('intercom', 'contact_history') }} as contact
WHERE
    true