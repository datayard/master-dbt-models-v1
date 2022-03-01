SELECT
    surveys.npsSurveyID,
    surveys.userId,
    surveys.organizationId,
    users.email,
    surveys.filled,
    surveys.cancelled,
    surveys.userScore,
    surveys.surveytype,
    surveys.userComment,
    surveys.createdDate
FROM 
    {{ ref('stg_vidyard_nps_surveys') }} as surveys
JOIN
    {{ ref('stg_vidyard_users') }} as users
on
    surveys.userid = users.userId
  WHERE TRUE
    AND surveys.userScore IS NOT null
    AND surveys.filled = true
    AND surveys.cancelled = false
    --AND surveys.surveytype IS null