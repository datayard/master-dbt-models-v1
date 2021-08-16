SELECT
    -- User Teams Folders
    utft2.userid
    , utft2.organizationid

    -- User Classification
    , uc.personal_account_type
    , uc.enterprise_access
    , uc.classification

    --email_to_exclude, domain, domain_type
    , vustg.excludeEmail
    , vustg.domain
    , vustg.domainType

    --org_name, created_date, updated_date, createdbyclientid, signup_source
    , vostg.name
    , vostg.createddate
    , vostg.updateddate
    , vostg.createdbyclientid
    , vostg.signup_source

    --first_view, first_view_videoid, total_seconds, videos_with_views, views_count
    , vomstg.firstviewdate
    , vomstg.firstviewvideoid
    , vomstg.totalseconds
    , vomstg.videoswithviews
    , vomstg.viewscount
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

