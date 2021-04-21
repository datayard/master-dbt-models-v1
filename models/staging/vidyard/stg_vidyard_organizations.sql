SELECT 
	vidyard_organizations.id as organizationID,
	vidyard_organizations.name as name,
	vidyard_organizations.org_type as orgtype,
	vidyard_organizations.parent_id as parentID,
	vidyard_organizations.locked as locked,
	vidyard_organizations.created_by_client_id as createdByClientID,
	vidyard_organizations.created_at as createdDate,
	vidyard_organizations.updated_at as updatedDate,
	vidyard_organizations.owner_id as ownerId,
	vidyard_organizations.locked_at as lockedAt,
	vidyard_organizations.account_id as accountId
 FROM 
	{{ source('public', 'vidyard_organizations') }} as vidyard_organizations