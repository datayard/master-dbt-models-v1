SELECT u.userid
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