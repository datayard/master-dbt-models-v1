with personal_account_type
    as (
        SELECT 
            utft2.*
            , CASE
                --CASE WHEN organizationid NOT IN (SELECT * from active_subscriptions) AND folder_type is "personal enterprise" THEN 'Enterprise'
                WHEN zt2.subscriptionid IS NULL AND folder_type = 'personal enterprise' THEN 'enterprise'

                --CASE WHEN organizationid IN (SELECT * from active_subscriptions) AND folder_type is "personal enterprise" THEN 'Enterprise Self-Serve'
                WHEN zt2.subscriptionid IS NOT NULL AND zt2.subscription_type LIKE 'Active - Self Serve Enterprise' AND folder_type = 'personal enterprise' THEN 'enterprise self_serve'

                --CASE WHEN organizationid NOT IN (SELECT * from active_subscriptions) AND folder_type LIKE "personal" THEN 'Free'
                WHEN zt2.subscriptionid IS NULL AND folder_type = 'personal' THEN 'free'

                --CASE WHEN organizationid NOT IN (SELECT * from active_subscriptions) AND folder_type LIKE "personal" THEN 'Free'
                WHEN zt2.subscriptionid IS NOT NULL AND zt2.subscription_type LIKE 'Active - Pro' AND folder_type = 'personal' THEN 'pro'

              END AS personal_account_type

        FROM 
            {{ ref('tier2_user_teams_folders') }} as utft2
            --dbt_vidyard_master.tier2_user_teams_folders as utft2
            LEFT JOIN {{ ref('tier2_zuora') }} as zt2
            --LEFT JOIN dbt_vidyard_master.tier2_zuora zt2
                ON zt2.vidyardid = utft2.organizationid

        WHERE utft2.orgtype LIKE 'self_serve'
          --AND utft2.accountid = 12449
    )
SELECT
    utft2.*
    , CASE

        --CASE WHEN userId IN (SELECT user_id from tm join t) AND t.isadmin = true then 'admin' -- Case for admins
        WHEN vuet2.entityid IS NOT NULL
                AND vuet2.isadmin = true
            THEN 'admin'

        --CASE WHEN userId IN (SELECT user_id, from tm join t) AND t.isadmin = false then 'user' -- case for users (NOTE USERS CAN ONLY BE IN 1 GROUP AT THE TIME SO NO DUPES EXPECTED)
        WHEN vuet2.entityid IS NOT NULL
                AND vuet2.isadmin = false
            THEN 'user'

        --CASE WHEN users IN (SELECT * from tm) AND tm.account id = parentid is "personal enterprise" THEN 'Enterprise'
        /*WHEN vuet2.entityid IS NOT NULL
                AND vuet2.organizationid = utft2.parentid
                    AND utft2.folder_type = 'personal enterprise'
            THEN 'enterprise'
        */
      END AS enterprise_access

    , CASE

        --CASE WHEN personal_account_type like 'Free' AND enterprise_access IS NULL then 'Free User'
        WHEN personal_account_type = 'free' AND enterprise_access IS NULL THEN 'free'

        --CASE WHEN personal_account_type like 'Pro' AND enterprise_access IS NULL then 'Pro User'
        WHEN personal_account_type = 'pro' AND enterprise_access IS NULL THEN 'pro'

        --CASE WHEN personal_account_type like 'Free' AND enterprise_access LIKE 'User' then 'Hybrid'
        WHEN personal_account_type = 'free' AND enterprise_access = 'user' THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Pro' AND enterprise_access LIKE 'User' then 'Hybrid'
        WHEN personal_account_type = 'pro' AND enterprise_access = 'user' THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Free' AND enterprise_access LIKE 'Admin' then 'Hybrid'
        WHEN personal_account_type = 'free' AND enterprise_access = 'admin' THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Pro' AND enterprise_access LIKE 'Admin' then 'Hybrid'
        WHEN personal_account_type = 'pro' AND enterprise_access = 'admin' THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Enterprise' AND enterprise_access LIKE 'User' then 'Enterprise User'
        WHEN personal_account_type = 'enterprise' AND enterprise_access = 'user' THEN 'enterprise user'

        --CASE WHEN personal_account_type like 'Enterprise' AND enterprise_access LIKE 'Admin' then 'Enterprise User'
        WHEN personal_account_type = 'enterprise' AND enterprise_access = 'admin' THEN 'enterprise user'

        --CASE WHEN personal_account_type like 'Enterprise' AND enterprise_access IS NULL then 'Anomaly'
        WHEN personal_account_type = 'enterprise' AND enterprise_access IS NULL THEN 'Anomaly'

        --CASE WHEN personal_account_type like 'Enterprise Self-serve' AND enterprise_access LIKE 'User' then 'Enterprise Self-Serve'
        WHEN personal_account_type = 'enterprise self_serve' AND enterprise_access = 'user' THEN 'enterprise self_serve'

        --CASE WHEN personal_account_type like 'Enterprise Self-serve' AND enterprise_access LIKE 'Admin' then 'Enterprise Self-Serve'
        WHEN personal_account_type = 'enterprise self_serve' AND enterprise_access = 'admin' THEN 'enterprise self_serve'

        --CASE WHEN personal_account_type like IS NULL AND enterprise_access IS NULL then 'Orphan'
        WHEN personal_account_type = 'enterprise self_serve' AND enterprise_access IS NULL THEN 'orphan'

        ELSE 'Unknown'
      END as classification

FROM
     --dbt_vidyard_master.tier2_user_teams_folders AS utft2
     personal_account_type utft2

     LEFT JOIN {{ ref('tier2_vidyard_user_entities') }} as vuet2
     --LEFT JOIN dbt_vidyard_master.tier2_vidyard_user_entities vuet2
        ON vuet2.entity = 'team'
               AND vuet2.userid = utft2.userid
               AND vuet2.organizationid = utft2.accountid
               AND vuet2.inviteaccepted = true

WHERE
    utft2.orgtype LIKE 'self_serve'
    --AND utft2.accountid = 12449
