SELECT
    conversation_contact.contact_id as contactID,
    conversation_contact.conversation_id as conversationID,
    conversation_contact.conversation_updated_at as coversationUpdatedAt
FROM
    {{ source('intercom', 'conversation_contact_history') }} as conversation_contact
WHERE
    TRUE