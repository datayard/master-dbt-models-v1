SELECT
    'team' AS entity                               
    , vud2.userid                                  
    , t.accountid AS organizationid                
    , t.teamid AS entityid                         
    , tm.teammembershipid AS childentityid         
    , tm.createddate as createddate                
    , tm.updateddate as updateddate                
    , vud2.orgType                       
    , tm.inviteaccepted                            
    , t.isadmin                                    
FROM
    {{ ref('tier2_vidyard_user_details') }} vud2
    JOIN {{ ref('stg_vidyard_team_memberships') }} tm
        ON tm.userid = vud2.userid
    JOIN {{ ref('stg_vidyard_teams') }} t
        ON t.teamid = tm.teamid