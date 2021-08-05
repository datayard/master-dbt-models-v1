with user_groups_teams as (
     with user_groups as (
         SELECT distinct u.userid
                       , ug.organizationid as organizationid
         FROM dbt_vidyard_master.stg_vidyard_users u
                  JOIN dbt_vidyard_master.stg_vidyard_user_groups ug
                       ON ug.userid = u.userid AND ug.inviteaccepted = TRUE
         --WHERE u.userid = 130902
     )
        , user_teams as (
         SELECT DISTINCT u.userid
                       , t.isadmin
                       , t.accountid
         FROM dbt_vidyard_master.stg_vidyard_users u
                  JOIN dbt_vidyard_master.stg_vidyard_team_memberships tm
                       ON tm.userid = u.userid
                  JOIN dbt_vidyard_master.stg_vidyard_teams t
                       ON tm.teamid = t.teamid
         --WHERE u.userid = 130902
     )
     SELECT ug.userid
          , ug.organizationid
          , ut.isadmin
     FROM user_groups ug
              LEFT JOIN user_teams ut
                        ON ug.userid = ut.userid and ug.organizationid = ut.accountid
 )
 SELECT vut2.*
      , CASE
            WHEN zt2.vidyardid IS NOT NULL THEN 1
            ELSE 0
     END as                                             subscription_active
      , CASE
            WHEN folder_type = 'personal' AND subscription_active = 0
                THEN 'free'
            WHEN folder_type = 'personal' AND subscription_active = 1
                THEN 'pro'
            WHEN folder_type = 'personal' AND subscription_active = 0
                THEN 'free'
            WHEN folder_type = 'personal' AND subscription_active = 1
                THEN 'pro'
            WHEN folder_type = 'personal' AND subscription_active = 0
                THEN 'free'
            WHEN folder_type = 'personal' AND subscription_active = 1
                THEN 'pro'

            WHEN folder_type = 'personal enterprise' AND subscription_active = 0
                THEN 'enterprise'
            WHEN folder_type = 'personal enterprise' AND subscription_active = 0
                THEN 'enterprise'
            WHEN folder_type = 'personal enterprise' AND subscription_active = 1
                THEN 'enterprise self_serve'
            WHEN folder_type = 'personal enterprise' AND subscription_active = 1
                THEN 'enterprise self_serve'
            WHEN folder_type IS NULL THEN 'orphan'
            ELSE
                NULL
     END AS                                             personal_account_type
      , CASE
            WHEN ugt.userid IS NOT NULL THEN 1
            ELSE 0
     END AS                                             has_valid_team_or_group_id
      , CASE WHEN ugt.isadmin IS TRUE THEN 1 ELSE 0 END is_user_admin
      , CASE
     --null    (user_id != membership on teams || group_id) & (team || group is admin = false)  & folder_type = personal
            WHEN has_valid_team_or_group_id = 0 AND is_user_admin = 0 AND
                 folder_type = 'personal' THEN NULL
     --null    (user_id != membership on teams || group_id) & (team || group is admin= true)  & folder_type = personal
            WHEN has_valid_team_or_group_id = 0 AND is_user_admin = 1 AND
                 folder_type = 'personal' THEN NULL
     --User    (user_id = membership on teams || group_id) & (team || group is admin= false) & folder_type = personal
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 0 AND
                 folder_type = 'personal' THEN 'user'
     --User    (user_id = membership on teams || group_id) & (team || group is admin= false) & folder_type = personal
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 0 AND
                 folder_type = 'personal' THEN 'user'
     --Admin   (user_id = membership on teams || group_id) & (team || group is admin= true) & folder_type = personal
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 1 AND
                 folder_type = 'personal' THEN 'admin'
     --Admin   (user_id = membership on teams || group_id) & (team || group is admin= true) & folder_type = personal
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 1 AND
                 folder_type = 'personal' THEN 'admin'

     --user    (user_id = membership on teams || group_id) & (team || group is admin= false) & folder_type = personal enterprise
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 0 AND
                 folder_type = 'personal enterprise' THEN 'user'
     --admin   (user_id = membership on teams || group_id) & (team || group is admin= true) & folder_type = personal enterprise
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 1 AND
                 folder_type = 'personal enterprise' THEN 'user'
     --User    (user_id = membership on teams || group_id) & (team || group is admin= false) & folder_type = personal enterprise
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 0 AND
                 folder_type = 'personal enterprise' THEN 'user'
     --admin   (user_id = membership on teams || group_id) & (team || group is admin= true) & folder_type = personal enterprise
            WHEN has_valid_team_or_group_id = 1 AND is_user_admin = 1 AND
                 folder_type = 'personal enterprise' THEN 'admin'
     --null    user_id not have a record on teams || group_id*/
            ELSE NULL
     END AS                                             enterprise_access
      , CASE
     --Free 	                NULL	            Free User
            WHEN personal_account_type = 'free' AND enterprise_access IS NULL
                THEN 'free user'
     --Pro	                    NULL	            Pro User
            WHEN personal_account_type = 'pro' AND enterprise_access IS NULL
                THEN 'pro user'
     --Free 	                User	            Hybrid
            WHEN personal_account_type = 'free' AND enterprise_access = 'user'
                THEN 'hybrid'
     --Pro	                    User	            Hybrid
            WHEN personal_account_type = 'pro' AND enterprise_access = 'user'
                THEN 'hybrid'
     --Free 	                Admin	            Hybrid
            WHEN personal_account_type = 'free' AND enterprise_access = 'admin'
                THEN 'hybrid'
     --Pro	                    Admin	            Hybrid
            WHEN personal_account_type = 'pro' AND enterprise_access = 'admin'
                THEN 'hybrid'
     --Enterprise	            NULL	            Unknown
            WHEN personal_account_type = 'enterprise' AND enterprise_access IS NULL
                THEN 'unknown'
     --Enterprise	            User	            Enterprise User
            WHEN personal_account_type = 'enterprise' AND enterprise_access = 'user'
                THEN 'enterprise user'
     --Enterprise	            Admin	            Enterprise User
            WHEN personal_account_type = 'enterprise' AND enterprise_access = 'admin'
                THEN 'enterprise user'
     --Enterprise Self-serve   User	            Enterprise Self-Serve
            WHEN personal_account_type = 'enterprise self_serve' AND
                 enterprise_access = 'user' THEN 'enterprise self_serve'
     --Enterprise Self-serve   Admin	            Enterprise Self-Serve
            WHEN personal_account_type = 'enterprise self_serve' AND
                 enterprise_access = 'admin' THEN 'enterprise self_serve'
     --NULL	                NULL	            Orphan
            WHEN personal_account_type IS NULL AND enterprise_access IS NULL
                THEN 'orphan'
            ELSE '--not classified'
     END AS                                             user_classification
 FROM dbt_vidyard_master.tier2_user_folders vut2
          LEFT JOIN dbt_vidyard_master.tier2_zuora zt2
                    ON zt2.vidyardid = vut2.organizationid
          LEFT JOIN user_groups_teams ugt
                    ON ugt.userid = vut2.userid AND ugt.organizationid = vut2.organizationid


