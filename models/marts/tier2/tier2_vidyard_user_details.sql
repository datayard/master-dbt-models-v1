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

SELECT
    -- User Teams Folders
    utft2.userid
    , utft2.organizationid
    , utft2.accountid

    -- User Classification
    , uc.personal_account_type
    , uc.enterprise_access
    , uc.classification

    --email_to_exclude, domain, domain_type
    , vustg.excludeEmail
    , vustg.domain
    , vustg.domainType
    , vustg.email

    --org_name, created_date, updated_date, createdbyclientid, signup_source
    , vostg.name
    , vostg.orgType
    , vostg.createddate
    , vostg.updateddate
    , vostg.createdbyclientid
    , vostg.signup_source
    , vostg.parentid

    --first_view, first_view_videoid, total_seconds, videos_with_views, views_count
    , vomstg.firstviewdate
    , vomstg.firstviewvideoid
    , vomstg.totalseconds
    , vomstg.videoswithviews
    , vomstg.viewscount
    , vomstg.activatedFlag

    -- last session date
    , lstsd.lastSessionDate
FROM
    --dbt_vidyard_master.tier2_user_teams_folders utft2
    {{ ref('tier2_user_teams_folders')}} as utft2

    --left join dbt_vidyard_master.tier2_users_classification uc
    LEFT JOIN {{ ref('tier2_users_classification') }} as uc
        on uc.userid = utft2.userid and uc.accountid = utft2.accountid

    --left join dbt_vidyard_master.stg_vidyard_users vustg
    LEFT JOIN {{ ref('stg_vidyard_users')}} as vustg
        on vustg.userid = utft2.userid

    --left join dbt_vidyard_master.stg_vidyard_organizations vostg
    LEFT JOIN {{ ref('stg_vidyard_organizations') }} as vostg
        on vostg.organizationid = utft2.organizationid

    --left join dbt_vidyard_master.stg_vidyard_org_metrics vomstg
    LEFT JOIN {{ ref('stg_vidyard_org_metrics') }} as vomstg
        on vomstg.organizationid = utft2.organizationid

    LEFT JOIN lastSessionDate as lstsd
        on lstsd.vidyardUserId = utft2.userid

