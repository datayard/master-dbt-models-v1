SELECT 
	vidyard_nps_surveys.id as npsSurveyID,
	vidyard_nps_surveys.user_score as userScore,
	vidyard_nps_surveys.user_comment as userComment,
	vidyard_nps_surveys.user_id as userId,
	vidyard_nps_surveys.organization_id as organizationId,
	vidyard_nps_surveys.created_at as createdDate,
	vidyard_nps_surveys.updated_at as updatedDate,
	vidyard_nps_surveys.filled as filled,
	vidyard_nps_surveys.allow_contact as allowContact,
	vidyard_nps_surveys.survey_type as surveyType,
	vidyard_nps_surveys.cancelled as cancelled
 FROM 
	{{ source('public', 'vidyard_nps_surveys') }} as vidyard_nps_surveys
