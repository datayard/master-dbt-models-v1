with personal_account_type
    as (
        SELECT 
            utft2.userid
            , utft2.accountid
            , CASE
                --CASE WHEN organizationid NOT IN (SELECT * from active_subscriptions) AND folder_type is "personal enterprise" THEN 'Enterprise'
                WHEN zt2.subscriptionid IS NULL AND lower(folder_type) = lower('personal enterprise') THEN 'enterprise'

                --CASE WHEN organizationid IN (SELECT * from active_subscriptions) AND folder_type is 'active - pro' THEN 'account to investigate’
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - pro') AND lower(folder_type) = lower('personal enterprise') THEN '--Under Investigation--'

                --CASE WHEN organizationid IN (SELECT * from active_subscriptions) AND folder_type is "personal enterprise" THEN 'Enterprise Self-Serve'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - self serve enterprise') AND lower(folder_type) = lower('personal enterprise') THEN 'enterprise self serve'

                --CASE WHEN organizationid NOT IN (SELECT * from active_subscriptions) AND folder_type LIKE "personal" THEN 'Free'
                WHEN zt2.subscriptionid IS NULL AND lower(folder_type) = lower('personal') THEN 'free'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) NOT LIKE lower('active %') AND lower(folder_type) = lower('personal') THEN 'free'

                --CASE WHEN organizationid NOT IN (SELECT * from active_subscriptions) AND folder_type LIKE "personal" THEN 'Free'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - pro') AND lower(folder_type) = lower('personal') THEN 'pro'

              END AS personal_account_type

        FROM 
            {{ ref('tier2_user_teams_folders') }} as utft2
            --dbt_vidyard_master.tier2_user_teams_folders as utft2
            LEFT JOIN {{ ref('tier2_zuora') }} as zt2
            --LEFT JOIN dbt_vidyard_master.tier2_zuora zt2
                ON zt2.vidyardAccountId = utft2.organizationid and lower(zt2.subscription_type) like lower('active %')

        WHERE utft2.orgtype LIKE 'self_serve'
          --AND utft2.accountid = 12449
    )
SELECT distinct
    pat.userid
    , pat.accountid
    , pat.personal_account_type
    , CASE

        --CASE WHEN userId IN (SELECT user_id from tm join t) AND t.isadmin = true then 'admin' -- Case for admins
        WHEN tm.teammembershipid IS NOT NULL AND tm.isadmin = true THEN 'admin'

        --CASE WHEN userId IN (SELECT user_id, from tm join t) AND t.isadmin = false then 'user' -- case for users (NOTE USERS CAN ONLY BE IN 1 GROUP AT THE TIME SO NO DUPES EXPECTED)
        WHEN tm.teammembershipid IS NOT NULL AND tm.isadmin = false THEN 'user'

      END AS enterprise_access

    , CASE

        --CASE WHEN personal_account_type like 'Free' AND enterprise_access IS NULL then 'Free User'
        WHEN lower(personal_account_type) = lower('free') AND enterprise_access IS NULL THEN 'free'

        --CASE WHEN personal_account_type like 'Pro' AND enterprise_access IS NULL then 'Pro User'
        WHEN lower(personal_account_type) = lower('pro') AND enterprise_access IS NULL THEN 'pro'

        --CASE WHEN personal_account_type like 'Free' AND enterprise_access LIKE 'User' then 'Hybrid'
        WHEN lower(personal_account_type) = lower('free') AND lower(enterprise_access) = lower('user') THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Pro' AND enterprise_access LIKE 'User' then 'Hybrid'
        WHEN lower(personal_account_type) = lower('pro') AND lower(enterprise_access) = lower('user') THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Free' AND enterprise_access LIKE 'Admin' then 'Hybrid'
        WHEN lower(personal_account_type) = lower('free') AND lower(enterprise_access) = lower('admin') THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Pro' AND enterprise_access LIKE 'Admin' then 'Hybrid'
        WHEN lower(personal_account_type) = lower('pro') AND lower(enterprise_access) = lower('admin') THEN 'hybrid'

        --CASE WHEN personal_account_type like 'Enterprise' AND enterprise_access LIKE 'User' then 'Enterprise User'
        WHEN lower(personal_account_type) = lower('enterprise') AND lower(enterprise_access) = lower('user') THEN 'enterprise user'

        --CASE WHEN personal_account_type like 'Enterprise' AND enterprise_access LIKE 'Admin' then 'Enterprise User'
        WHEN lower(personal_account_type) = lower('enterprise') AND lower(enterprise_access) = lower('admin') THEN 'enterprise user'

        --CASE WHEN personal_account_type like 'Enterprise' AND enterprise_access IS NULL then 'Anomaly'
        WHEN lower(personal_account_type) = lower('enterprise') AND enterprise_access IS NULL THEN 'anomaly'

        --CASE WHEN personal_account_type like 'Enterprise Self-serve' AND enterprise_access LIKE 'User' then 'Enterprise Self-Serve'
        WHEN lower(personal_account_type) = lower('enterprise self serve') AND lower(enterprise_access) = lower('user') THEN 'enterprise self serve'

        --CASE WHEN personal_account_type like 'Enterprise Self-serve' AND enterprise_access LIKE 'Admin' then 'Enterprise Self-Serve'
        WHEN lower(personal_account_type) = lower('enterprise self serve') AND lower(enterprise_access) = lower('admin') THEN 'enterprise self serve'

        --CASE WHEN personal_account_type like IS NULL AND enterprise_access IS NULL then 'Orphan'
        WHEN personal_account_type IS NULL AND enterprise_access IS NULL THEN 'orphan'

        ELSE 'unknown'
      END as classification

FROM
     --dbt_vidyard_master.tier2_user_teams_folders AS utft2
     personal_account_type pat

    -- LEFT JOIN {{ ref('stg_vidyard_team_memberships') }} as tm
    --     ON tm.userid = pat.userid
    -- LEFT JOIN {{ ref('stg_vidyard_teams') }} as t
    --     ON t.teamid = tm.teamid
    --         AND t.accountid = pat.accountid
    --         AND tm.inviteaccepted = true
    LEFT JOIN {{ ref('tier2_vidyard_team_memberships') }} as tm
        ON tm.userid = pat.userid
        and tm.inviteaccepted = true

