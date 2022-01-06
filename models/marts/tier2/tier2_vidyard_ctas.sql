WITH
  total_ctas as (
    SELECT
      vyorgs.accountId,
      COUNT (DISTINCT vyevents.eventID) as cta
    FROM
        {{ ref('stg_vidyard_events') }} as vyevents
    JOIN
        {{ ref('stg_vidyard_organizations') }} as vyorgs
    ON
      vyevents.organizationID = vyorgs.organizationID
    WHERE
      true
    GROUP BY
      vyorgs.accountId
    ),
   
  ctas_applied as (

    SELECT
      vyorgs.accountId as accountid,
      COUNT (distinct vyeventsjoins.eventID) as cta_applied
    FROM
      {{ ref('stg_vidyard_events') }} as vyevents
    JOIN
      {{ ref('stg_vidyard_event_joins') }} as vyeventsjoins
    ON
      vyevents.eventID = vyeventsjoins.eventID
    JOIN
      {{ ref('stg_vidyard_organizations') }} as vyorgs
    ON
      vyevents.organizationID = vyorgs.organizationID
    WHERE
      true
    GROUP BY
      vyorgs.accountId
)

SELECT  
  vyorgs.accountId,
  total_ctas.cta,
  ctas_applied.cta_applied
FROM
  total_ctas
JOIN
    {{ ref('stg_vidyard_organizations') }} as vyorgs
ON
  total_ctas.accountId = vyorgs.organizationID
LEFT JOIN
  ctas_applied
ON
  vyorgs.accountId = ctas_applied.accountid
WHERE
  true
