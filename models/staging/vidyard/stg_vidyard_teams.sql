SELECT 
	vidyard_teams.id as teamID,
	vidyard_teams.account_id as accountID,
	vidyard_teams.is_admin as isAdmin
 FROM 
	{{ source('public', 'vidyard_teams') }} as vidyard_teams