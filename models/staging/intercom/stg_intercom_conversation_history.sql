SELECT
    conversation.id as conversationId,
    conversation.updated_at as updatedDate,
    conversation.source_url as sourceURL,
    conversation.created_at as createdDate
FROM
    {{ source('intercom', 'conversation_history') }} as conversation
WHERE
    true