SELECT 
	vidyard_roles.id as userID,
    vidyard_roles.organization_id as organizationID,
    vidyard_roles.role_type as roleType,
    vidyard_roles.can_create_roles as canCreateRoles

 FROM 
	{{ source('public', 'vidyard_roles') }} as vidyard_roles