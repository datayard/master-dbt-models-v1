SELECT                               
    tm.userid                             
    , t.accountid             
    , t.teamid                         
    , tm.teammembershipid    
    , tm.createddate                
    , tm.updateddate as inviteaccepteddate                          
    , tm.inviteaccepted                            
    , t.isadmin                                    
FROM
    {{ ref('stg_vidyard_teams') }} t
    LEFT JOIN {{ ref('stg_vidyard_team_memberships') }} tm
        ON t.teamid = tm.teamid
