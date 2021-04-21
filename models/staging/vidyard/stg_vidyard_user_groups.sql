SELECT 
	vidyard_user_groups.id as groupID,
	vidyard_user_groups.user_id as userID,
	vidyard_user_groups.organization_id as organizationID,
	vidyard_user_groups.invite_accepted as inviteAccepted,
	vidyard_user_groups.created_at as createdDate,
	vidyard_user_groups.updated_at as updatedDate
 FROM 
	{{ source('public', 'vidyard_user_groups') }} as vidyard_user_groups