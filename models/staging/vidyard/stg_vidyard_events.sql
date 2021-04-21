SELECT
        vidyard_events.id as eventID,
        vidyard_events.event_type as eventType,
        vidyard_events.organization_id as organizationID,
        vidyard_events.created_at as createdDate,
        vidyard_events.updated_at as updatedDate
FROM
    {{ source ('public','vidyard_events')}} as vidyard_events