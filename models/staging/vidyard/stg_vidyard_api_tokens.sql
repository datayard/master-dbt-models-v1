SELECT 
	vidyard_tokens.id as activeTokenID,
    vidyard_tokens.organization_id as organizationID,
    vidyard_tokens.token_type as tokenType,
    vidyard_tokens.uuid as uuid,
    vidyard_tokens.data as tokendata,
    vidyard_tokens.created_at as createdAt,
    vidyard_tokens.updated_at as updatedAt,
    vidyard_tokens.is_valid as isValid
 FROM 
	{{ source('public', 'vidyard_api_tokens') }} as vidyard_tokens