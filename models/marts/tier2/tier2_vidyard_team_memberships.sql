SELECT                             
    -- vud2.userid     
    tm.userid                             
    , t.accountid             
    , t.teamid                         
    , tm.teammembershipid    
    , tm.createddate                
    , tm.updateddate as inviteaccepteddate               
    -- , vud2.orgtype                       
    , tm.inviteaccepted                            
    , t.isadmin                                    
FROM
    {{ ref('stg_vidyard_teams') }} t
    LEFT JOIN {{ ref('stg_vidyard_team_memberships') }} tm
        ON t.teamid = tm.teamid
    -- LEFT JOIN {{ ref('tier2_vidyard_user_details') }} vud2    
    --     ON tm.userid = vud2.userid 
