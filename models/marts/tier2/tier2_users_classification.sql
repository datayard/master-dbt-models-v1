with personal_account_type
    as (
        SELECT 
            utft2.userid
            , utft2.accountid
            , CASE
                WHEN zt2.subscriptionid IS NULL AND lower(folder_type) = lower('personal enterprise') THEN 'enterprise'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - pro') AND lower(folder_type) = lower('personal enterprise') THEN '--Under Investigation--'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - self serve enterprise') AND lower(folder_type) = lower('personal enterprise') THEN 'enterprise self serve'
                WHEN zt2.subscriptionid IS NULL AND lower(folder_type) = lower('personal') THEN 'free'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) NOT LIKE lower('active %') AND lower(folder_type) = lower('personal') THEN 'free'
                WHEN zt2.subscriptionid IS NOT NULL AND lower(zt2.subscription_type) LIKE lower('active - pro') AND lower(folder_type) = lower('personal') THEN 'pro'
              END AS personal_account_type
        FROM 
            {{ ref('tier2_user_teams_folders') }} as utft2
            LEFT JOIN {{ ref('tier2_zuora') }} as zt2
                ON zt2.vidyardAccountId = utft2.organizationid and lower(zt2.subscription_type) like lower('active %')
        WHERE utft2.orgtype LIKE 'self_serve'
    )

SELECT distinct
    pat.userid
    , pat.accountid
    , pat.personal_account_type
    , CASE
        WHEN tm.teamid IS NOT NULL AND tm.isadmin = true THEN 'admin'
        WHEN tm.teamid IS NOT NULL AND tm.isadmin = false THEN 'user'
      END AS enterprise_access
    , CASE
        WHEN lower(personal_account_type) = lower('free') AND enterprise_access IS NULL THEN 'free'
        WHEN lower(personal_account_type) = lower('pro') AND enterprise_access IS NULL THEN 'pro'
        WHEN lower(personal_account_type) in ('free','pro') AND enterprise_access IS NOT NULL THEN 'hybrid'
        WHEN lower(personal_account_type) = lower('enterprise') AND enterprise_access IS NOT NULL THEN 'enterprise user'
        WHEN lower(personal_account_type) = lower('enterprise') AND enterprise_access IS NULL THEN 'anomaly'
        WHEN lower(personal_account_type) = lower('enterprise self serve') AND enterprise_access IS NOT NULL THEN 'enterprise self serve'
        WHEN personal_account_type IS NULL AND enterprise_access IS NULL THEN 'orphan'
        ELSE 'unknown'
      END as classification
FROM
     personal_account_type pat
    LEFT JOIN {{ ref('tier2_vidyard_team_memberships') }} as tm
        ON tm.userid = pat.userid
        and tm.inviteaccepted = true

