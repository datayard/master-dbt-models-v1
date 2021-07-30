WITH user_groups AS (
    SELECT u.userid,
           o.organizationid,
           ug.groupid
    FROM dbt_vidyard_master.stg_vidyard_users u
             JOIN dbt_vidyard_master.stg_vidyard_organizations o
                  ON o.ownerid = u.userid
             JOIN dbt_vidyard_master.stg_vidyard_user_groups ug
                  ON ug.userid = u.userid and ug.organizationid = o.organizationid and ug.inviteaccepted = true
)
, user_teams AS (
    SELECT u.userid,
           o.organizationid,
           t.teamid
    FROM dbt_vidyard_master.stg_vidyard_users u
             JOIN dbt_vidyard_master.stg_vidyard_organizations o
                  ON o.ownerid = u.userid
             JOIN dbt_vidyard_master.stg_vidyard_team_memberships tm
                  ON tm.userid = u.userid AND tm.inviteaccepted = TRUE
             JOIN dbt_vidyard_master.stg_vidyard_teams t
                  ON t.teamid = tm.teamid AND t.accountid IS NOT NULL
)
SELECT
    u.userid
    , u.email
    , u.email_to_exclude
    , u.domain
    , u.domain_type
    , o.organizationid
    , o.ownerid
    , o.accountid
    , o.orgtype
  	, o.parentid
  	, o.name as org_name
  	, o.createddate
  	, o.updateddate
  	, o.createdbyclientid
  	, o.paying
  	, o.signup_source
    , om.firstview
    , om.firstviewvideoid
    , om.totalseconds
    , om.videoswithviews
    , om.viewscount
    , ug.groupid
    , ut.teamid
    , CASE
        WHEN COALESCE(ug.groupid, ut.teamid) IS NOT NULL THEN 1
        ELSE 0
      END AS has_access_to_enterprise_account
    , CASE
           WHEN u.userid = o.ownerid and o.orgtype = 'self_serve' THEN 1
           ELSE 0
      END AS has_personal_account
    , CASE
           WHEN u.userid = ownerid and o.organizationid != o.accountid and o.orgtype = 'self_serve' THEN 1
           ELSE 0
      END AS linked_to_parent_account
    , CASE
        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 0 THEN 'Orphan'
        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 1 THEN 'Standalone'
        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 2 THEN 'Enterprise Only'
        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 3 THEN 'Hybrid'
        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 7 THEN 'Enterprise Personal'

        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 4 THEN 'Linked to Parent Account Only'
        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 5 THEN 'Has Personal Account & Linked to Parent Account'
        WHEN ((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                 (linked_to_parent_account * 4)) = 6
               THEN 'Linked to Parent Account & Has access to Enterprise Account'

        ELSE CAST(((has_personal_account * 1) + (has_access_to_enterprise_account * 2) +
                      (linked_to_parent_account * 4)) AS VARCHAR(5))
      END as user_type
FROM
     {{ ref('stg_vidyard_users') }} u
     LEFT JOIN {{ ref('stg_vidyard_organizations') }} o
        ON o.ownerid = u.userid
     LEFT JOIN {{ ref('stg_vidyard_org_metrics') }} om
         ON om.organizationid = o.organizationid
     LEFT JOIN user_groups ug
        ON ug.userid = u.userid and ug.organizationid = o.organizationid
     LEFT JOIN user_teams ut
        ON ut.userid = u.userid and ut.organizationid = o.organizationid