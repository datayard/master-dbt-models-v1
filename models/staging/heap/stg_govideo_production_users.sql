SELECT
        users.user_id as userID,
        users."identity" as identifier,
        --cast("identity" as varchar(10)) as vidyardUserId
        users.appcuesuserid as appcuesUserID,
        users.joindate as joinDate,
        users.last_modified as lastModifiedDate,
        users.video_completion as videoCompletion,
        users.extensionversion as extensionVersion,
        users.camera_enabled as cameraEnabled,
        users.appversion as appVersion,
        users.microphone_enabled as microphoneEnabled,
        users.camera_allowed as cameraAllowed,
        users.microphone_allowed as microphoneAllowed,
        users.mirrored_camera as mirroredCamera,
        users.general_use_case as generalUseCase,
        users.specific_use_case as specificUseCase,
        users.tailored_onboarding_use_case_response as onboardingUsecaseResponse

FROM
        {{ source('govideo_production' , 'users')}} as users

WHERE
        TRUE