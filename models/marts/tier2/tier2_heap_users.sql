SELECT
        users.userID,
        users.identifier,
        users.vidyardUserId,
        users.appcuesUserID,
        users.joinDate,
        users.lastModifiedDate,
        users.videoCompletion,
        users.extensionVersion,
        users.cameraEnabled,
        users.appVersion,
        users.microphoneEnabled,
        users.cameraAllowed,
        users.microphoneAllowed,
        users.mirroredCamera,
        users.generalUseCase,
        users.specificUseCase,
        users.onboardingUsecaseResponse, 
        users.usecase,
        users.usecase_c, 
        users.combined_usecase,
        users.confidence_survey
FROM
        {{ ref('stg_govideo_production_users')}} as users
