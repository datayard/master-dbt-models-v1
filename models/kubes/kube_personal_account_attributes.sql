WITH personal_accounts as (
    SELECT
        utf.userid,
        utf.email,
        utf.organizationid,
        utf.orgtype,
        utf.folder_type
    FROM
        {{ ref('tier2_user_teams_folders') }} as utf
        --dbt_vidyard_master.tier2_user_teams_folders as utf
    WHERE
        utf.orgtype LIKE 'self_serve'
), personal_account_videos as (
    SELECT
        count(vyv.videoid) as videos_created,
        vyv.organizationid
    FROM
        {{ ref('stg_vidyard_videos') }} as vyv
        --dbt_vidyard_master.stg_vidyard_videos as vyv
    WHERE
        source NOT LIKE 'sample'
     GROUP BY 2
)

SELECT
    pa.userid,
    pa.email,
    vyo.name,
    pa.organizationid,
    pa.orgtype,
    pa.folder_type,
    personal_account_videos.videos_created,
    vyom.viewscount,
    vyom.totalseconds / 60 as min_watched
FROM
    personal_accounts as pa
JOIN
    {{ ref('stg_vidyard_organizations') }} as vyo
    --dbt_vidyard_master.stg_vidyard_organizations as vyo
ON
    vyo.organizationid = pa.organizationid
LEFT JOIN
    {{ ref('stg_vidyard_org_metrics') }} as vyom
    --dbt_vidyard_master.stg_vidyard_org_metrics as vyom
ON
    vyom.organizationid = vyo.organizationid
JOIN
    personal_account_videos
ON
    personal_account_videos.organizationid = vyo.organizationid




