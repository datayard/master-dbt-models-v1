SELECT
	npsSurveyID,
	userScore,
	userComment,
	userId,
	organizationId,
	createdDate,
	updatedDate,
	filled,
	allowContact,
	surveyType,
	cancelled,
	props
FROM
    {{ ref('stg_vidyard_nps_surveys') }}