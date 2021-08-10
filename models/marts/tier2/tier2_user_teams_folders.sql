SELECT
    u.userid
    , o.ownerid
    , o.organizationid
    , o.accountid
    , o.parentid
    , o.orgtype
    , u.email
    , CASE
        WHEN o.orgtype = 'self_serve' and o.organizationid = o.accountid
            THEN 'personal'
        WHEN o.orgtype IS NULL and o.organizationid != o.accountid
            THEN 'shared'
        WHEN o.orgtype IS NULL and o.organizationid = o.accountid
            THEN 'parent folder'
        WHEN o.orgtype = 'self_serve' and o.organizationid != o.accountid
            THEN 'personal enterprise'
        WHEN o.orgtype IS NULL and o.organizationid IS NULL
            THEN 'orphan'
        ELSE NULL
    END AS folder_type
FROM dbt_vidyard_master.stg_vidyard_users u
        LEFT JOIN dbt_vidyard_master.stg_vidyard_organizations o
                ON o.ownerid = u.userid
WHERE
    ((o.orgtype = 'self_serve' and o.organizationid = o.accountid)
    OR (o.orgtype IS NULL and o.organizationid != o.accountid)
    OR (o.orgtype = 'self_serve' and o.organizationid != o.accountid)
    OR (o.orgtype IS NULL and o.organizationid IS NULL))

UNION ALL

SELECT
    u.userid
    , NULL AS ownerid
    , NULL AS organizationid
    , accountid
    , NULL AS parentid
    , NULL AS orgtype
    , u.email
    , 'parent folder' as folder_type
FROM
    dbt_vidyard_master.stg_vidyard_users u
    JOIN dbt_vidyard_master.stg_vidyard_team_memberships tm
        ON tm.userid = u.userid and tm.inviteaccepted = true
    JOIN dbt_vidyard_master.stg_vidyard_teams t
        ON t.teamid = tm.teamid
