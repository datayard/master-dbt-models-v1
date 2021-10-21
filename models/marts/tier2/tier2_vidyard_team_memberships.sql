WITH team_memberships AS (
    SELECT tm.userid
         , t.accountid
         , t.teamid
         , tm.teammembershipid
         , tm.createddate
         , tm.updateddate     AS inviteaccepteddate
         , tm.inviteaccepted
         , t.isadmin
    FROM     
        {{ ref('stg_vidyard_teams') }} t
        LEFT JOIN {{ ref('stg_vidyard_team_memberships') }} tm
            ON t.teamid = tm.teamid
)
, tier2_vidyard_user_groups AS (
    SELECT
        ug.userid
        , ug.groupid
        , ug. organizationid
        , o.accountid
        , o.ownerid
        , o.createdbyclientid
        , ug.inviteaccepted
        , ug.createddate
        , ug.updateddate
        , CASE WHEN vr.roletype = 'group_admin' THEN true ELSE false END AS isadmin
    FROM {{ ref('stg_vidyard_user_groups') }} ug
    LEFT JOIN {{ ref('stg_vidyard_organizations') }} o
        ON ug.organizationid = o.organizationid
    LEFT JOIN {{ ref('stg_vidyard_roles') }} vr
        ON ug.userid = vr.userid
    /* lines 34 - 37 filtering out user ids that already have a team membership so there are no duplicate user ids*/
    LEFT JOIN team_memberships tm
        ON ug.userid = tm.userid
    WHERE
        tm.userid is null
    /* line 39 adding this where clause will filter out any self serve users that are likely free*/
        AND o.ownerid != ug.userid
    )
    
    SELECT userid
         , accountid
         , teamid
         , teammembershipid
         , createddate
         , inviteaccepteddate
         , inviteaccepted
         , isadmin
         , 'team_memberships' AS source
    FROM team_memberships
    UNION
    SELECT userid
         , accountid
         , groupid       AS teamid
         , null          AS teammembershipid
         , createddate
         , updateddate   AS inviteaccepteddate
         , inviteaccepted
         , isadmin
         , 'user_groups' AS source
    FROM tier2_vidyard_user_groups g
