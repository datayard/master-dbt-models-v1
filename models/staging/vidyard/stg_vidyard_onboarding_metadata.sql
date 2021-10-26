SELECT 
	vidyard_onboarding_metadata.id as onboardingID,
    vidyard_onboarding_metadata.user_id as userID,
    vidyard_onboarding_metadata.new_user_survey as newUserSurvey,
    cast(vidyard_onboarding_metadata.created_at as date) as createdDate,
    cast(vidyard_onboarding_metadata.updated_at as date) as updatedDate,
    vidyard_onboarding_metadata.general_use_case as generalUseCase

 FROM 
	{{ source('public', 'vidyard_onboarding_metadata') }} as vidyard_onboarding_metadata