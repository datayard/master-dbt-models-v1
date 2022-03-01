/*
Weekly “Engaged” User is a business user who has performed at least 1 “key action” in the past week.
Key actions include
    (i) created a video
    (ii) shared a video
    (iii) managed content
    (iv) used analytics
        and
    (v) performed an admin action.
The metric is intended to capture the sales, manager, comms, and broader marketing uses cases.
Excludes personal and Ed users.
*/

WITH heap_data AS (
    SELECT
        heap.vidyardUserId
        , heap.tracker AS event
        , max(heap.eventtime) AS lastsession
    FROM {{ ref('tier2_heap') }} AS heap
    WHERE heap.tracker in (
                'video_creation_create_combo'   -- video creation
                , 'sharing_share_combo'         -- share video
                , 'manage_combo'                -- content management
                , 'insights_analytics_combo'    -- used analytics
                , 'admin_combo'                 -- admin action
        )
        and heap.eventtime >= DATEADD(DAY, -7, current_date)
    GROUP BY 1, 2
    ORDER BY 1
)
SELECT
    usrdet.userid
    , hd.event
    , hd.lastsession
    , usrdet.organizationid
    , usrdet.accountid
    , usrdet.personal_account_type
    , usrdet.enterprise_access
    , usrdet.classification
    , usrdet.excludeEmail
    , usrdet.domain
    , usrdet.domainType
    , usrdet.viewscount
FROM
    {{ ref('tier2_vidyard_user_details') }} AS usrdet
    --"dev"."dbt_vidyard_master"."tier2_vidyard_user_details" AS usrdet
    JOIN heap_data hd
        ON hd.vidyarduserid = usrdet.userid

