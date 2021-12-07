with personal_account_type
    as (
        SELECT DISTINCT
            utft2.userid
            , utft2.accountid
            , utft2.organizationid
            , utft2.folder_type
            , utft2.signup_source
            , CASE
                WHEN zt2.subscriptionid IS NULL AND lower(folder_type) = lower('personal enterprise') THEN 'enterprise'
                WHEN zt2.subscriptionid IS NULL AND lower(folder_type) = lower('personal') THEN 'free'
                WHEN zt2.subscriptionid IS NOT NULL AND zt2.subscription_type IN ('Cancelled - Pro','Cancelled - Self Serve Enterprise') AND lower(folder_type) = lower('personal') THEN 'free'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - self serve enterprise') AND lower(folder_type) = lower('personal enterprise') THEN 'enterprise self serve'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - pro') AND lower(folder_type) = lower('personal') THEN 'pro'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - pro') AND lower(folder_type) = lower('personal enterprise') THEN 'pro enterprise'
                WHEN zt2.subscriptionid IS NOT NULL AND (zt2.subscription_type) IN ('Active - Others', '--not yet classified--') THEN 'investigate enterprise'
                ELSE 'investigate'
              END AS personal_account_type
        FROM
                -- dbt_vidyard_master.tier2_user_teams_folders as utft2
                {{ ref('tier2_user_teams_folders') }} as utft2
                -- LEFT JOIN dbt_vidyard_master.tier2_zuora zt2
                LEFT JOIN {{ ref('tier2_zuora') }} as zt2
                ON zt2.vidyardAccountId = utft2.organizationid and (lower(zt2.subscription_type) like lower('active %') or zt2.subscription_type = '--not yet classified--')
                AND zt2.status in ('Active','Cancelled')
    )
    SELECT DISTINCT
                    pat.userid as userid
                  , pat.accountid as accountid
                  , pat.organizationid
                  , pat.signup_source
                  , pat.personal_account_type
                  , case when tm.teamid IS NOT NULL then true
                      else false
                    end as enterprise_access
                  , CASE
                        WHEN lower(personal_account_type) = lower('free') AND tm.teamid IS NULL THEN 'free'
                        WHEN lower(personal_account_type) = lower('pro') AND tm.teamid IS NULL THEN 'pro'
                        WHEN lower(personal_account_type) in ('free', 'pro') AND tm.teamid IS NOT NULL THEN 'hybrid'
                        WHEN lower(personal_account_type) = 'pro enterprise' AND tm.teamid IS NOT NULL THEN 'enterprise user'
                        WHEN lower(personal_account_type) = lower('enterprise') AND tm.teamid IS NOT NULL THEN 'enterprise user'
                        WHEN lower(personal_account_type) = lower('enterprise') AND tm.teamid IS NULL THEN 'missing teamid'
                        WHEN lower(personal_account_type) = lower('enterprise self serve') AND tm.teamid IS NOT NULL THEN 'enterprise self serve'
                        WHEN lower(personal_account_type) = lower('enterprise self serve') AND tm.teamid IS NOT NULL THEN 'missing teamid'
                        WHEN personal_account_type IS NULL AND tm.teamid IS NULL THEN 'orphan'
                        ELSE 'under investigation'
                    END as classification
    FROM personal_account_type pat
            --  LEFT JOIN dbt_vidyard_master.tier2_vidyard_team_memberships as tm
            LEFT JOIN {{ ref('tier2_vidyard_team_memberships') }} as tm
                       ON tm.userid = pat.userid
                           and tm.inviteaccepted = true