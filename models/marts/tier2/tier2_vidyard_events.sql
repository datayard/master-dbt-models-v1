SELECT
        vidyard_event_joins.eventJoinID,
        vidyard_event_joins.eventID,
        vidyard_event_joins.ownerType,
        vidyard_event_joins.ownerID,
        vidyard_event_joins.second,
        vidyard_event_joins.duration,
        vidyard_event_joins.createdDate,
        vidyard_event_joins.updatedDate,
        vidyard_events.eventType,
        vidyard_events.organizationID,
        vidyard_events.CreatedDate as eventCreatedDate,
        vidyard_events.UpdatedDate as eventUpdatedDate
FROM
    {{ ref('stg_vidyard_event_joins') }} as vidyard_event_joins
    left join {{ ref('stg_vidyard_events') }} as vidyard_events
    on vidyard_event_joins.eventid = vidyard_events.eventid