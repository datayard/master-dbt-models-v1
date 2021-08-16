SELECT 
	vidyard_organizations.id as organizationID,
	vidyard_organizations.name as name,
	vidyard_organizations.org_type as orgType,
	vidyard_organizations.parent_id as parentID,
	vidyard_organizations.locked as locked,
	vidyard_organizations.created_by_client_id as createdByClientID,
	vidyard_organizations.created_at as createdDate,
	vidyard_organizations.updated_at as updatedDate,
	vidyard_organizations.owner_id as ownerId,
	vidyard_organizations.locked_at as lockedDate,
	vidyard_organizations.account_id as accountId,
	vidyard_organizations.paying as paying
	, case
		when vidyard_organizations.created_by_client_id = 'android.vidyard.com'
			then 'Android'
		when vidyard_organizations.created_by_client_id = 'govideo-mobile.vidyard.com'
			then 'iOS'
		when vidyard_organizations.created_by_client_id = 'app.slack.com'
			then 'Slack'
		when vidyard_organizations.created_by_client_id = 'edge-extension.vidyard.com'
			then 'Edge'
		when vidyard_organizations.created_by_client_id = 'Enterprise'
			then 'Enterprise'
		when vidyard_organizations.created_by_client_id IN ('app.hubspot.com','marketing.hubspot.com','marketing.hubspotqa.com')
			then 'Hubspot'
		when vidyard_organizations.created_by_client_id NOT IN ('secure.vidyard.com','auth.viewedit.com','edge-extension.vidyard.com')
			then 'Partner App'
		else null
	end as signup_source
 FROM 
	{{ source('public', 'vidyard_organizations') }} as vidyard_organizations