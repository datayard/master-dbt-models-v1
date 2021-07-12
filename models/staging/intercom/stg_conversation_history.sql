SELECT
    conversation.id as conversationId,
    conversation.updated_at as updatedAt,
    conversation.source_url as sourceURL
FROM
    intercom.conversation_history as conversation
WHERE
    true