SELECT
    surveys.npsSurveyID,
    surveys.userId,
    surveys.organizationId,
    users.email,
    surveys.filled,
    surveys.cancelled,
    surveys.userScore,
    surveys.userComment,
    surveys.createdAt
FROM 
    {{ ref('stg_vidyard_nps_surveys') }} as surveys
JOIN
    {{ ref('stg_vidyard_users') }} as users
on
    surveys.userid = users.userId
  WHERE TRUE
    --AND surveys.createdAt >= DATEADD(day,-7,CURRENT_DATE)
    AND surveys.createdAt < CURRENT_DATE
    AND surveys.userScore IS NOT null
    AND surveys.filled = true
    AND surveys.cancelled = false