SELECT 
	vidyard_team_memberships.id as teamMembershipID,
	vidyard_team_memberships.team_id as teamID,
	vidyard_team_memberships.user_id as userID,
	vidyard_team_memberships.invite_accepted as inviteAccepted,
	vidyard_team_memberships.created_at as createdDate,
	vidyard_team_memberships.updated_at as updatedDate
 FROM 
	{{ source('public', 'vidyard_team_memberships') }} as vidyard_team_memberships