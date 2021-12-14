WITH lastSessionDate as (
    SELECT
            tier2heap.vidyarduserid,
            MAX (tier2heap.sessionTime) as lastSessionDate
        FROM
            dbt_vidyard_master.tier2_heap as tier2heap
        WHERE
            tier2heap.tracker = 'global_session'
            OR tier2heap.tracker = 'opened_extension'
            OR tier2heap.tracker = 'product_sessions'
        GROUP BY
            1
)

SELECT DISTINCT
    -- User Teams Folders
    utft2.userid
    , utft2.organizationid
    , utft2.accountid

    -- User Classification
    , uc.personal_account_type
    , uc.enterprise_access
    , uc.classification

    --email_to_exclude, domain, domain_type
    , utft2.excludeEmail
    , utft2.domain
    , utft2.domainType
    , utft2.email

    --org_name, created_date, updated_date, createdbyclientid, signup_source
    , utft2.name
    , utft2.orgType
    , utft2.createddate
    , utft2.updateddate
    , utft2.createdbyclientid
    , utft2.signup_source
    , utft2.parentid

    --first_view, first_view_videoid, total_seconds, videos_with_views, views_count
    , utft2.firstviewdate
    , utft2.firstviewvideoid
    , utft2.totalseconds
    , utft2.videoswithviews
    , utft2.viewscount
    , utft2.activatedFlag

    -- last session date
    , lstsd.lastSessionDate
FROM
    --dbt_vidyard_master.tier2_user_teams_folders utft2
    {{ ref('tier2_user_teams_folders')}} as utft2

    --left join dbt_vidyard_master.tier2_users_classification uc
    LEFT JOIN {{ ref('tier2_users_classification') }} as uc
        on uc.userid = utft2.userid and uc.accountid = utft2.accountid

    LEFT JOIN lastSessionDate as lstsd
        on lstsd.vidyardUserId = utft2.userid