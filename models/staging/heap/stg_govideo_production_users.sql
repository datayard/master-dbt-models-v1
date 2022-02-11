SELECT
        users.user_id as userID,
        users."identity" as identifier,
        cast(case when REGEXP_COUNT(identifier, '^[0-9]+$') = 1 then identifier else null end as integer) as vidyardUserId,
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
        users.tailored_onboarding_use_case_response as onboardingUsecaseResponse,
        users.usecase,
        case
                when users.usecase ilike '%skip%' then null
                when users.usecase ilike '%customer%' then 'customer-success'
                when users.usecase ilike '%other%' and users.general_use_case is not null and users.general_use_case not ilike'%skip%' then null
                else users.usecase
        end as usecase_c,
        case
                when users.general_use_case ilike '%skip%' then null
                when users.general_use_case ilike '%customer%' then 'customer-success'
                when users.general_use_case ilike '%other%' and users.usecase is not null and users.usecase not ilike'%skip%' then null
                else users.general_use_case
        end as general_use_case_c,
        lower(coalesce(general_use_case_c,usecase_c)) as combined_usecase,
       users.confidence_survey

FROM
        {{ source('govideo_production' , 'users')}} as users

WHERE
        TRUE