SELECT
        vidyard_event_joins.id as eventJoinID,
        vidyard_event_joins.event_id as eventID,
        vidyard_event_joins.owner_type as ownerType,
        vidyard_event_joins.owner_id as ownerID,
        vidyard_event_joins.second as second,
        vidyard_event_joins.duration as duration,
        vidyard_event_joins.created_at as createdDate,
        vidyard_event_joins.updated_at as updatedDate
FROM
    {{ source ('public','vidyard_event_joins')}} as vidyard_event_joins