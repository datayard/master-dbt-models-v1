SELECT 
	vidyard_players.id as playerID,
	vidyard_players.organization_id as organizationID,
	vidyard_players.owner_id as ownerID,
	vidyard_players.created_by_client_id as createdbyclientID,
	vidyard_players.created_at as createdDate,
	vidyard_players.updated_at as updatedDate,
	vidyard_players.player_type as playerType,
	vidyard_players.uuid as uuID
 FROM 
	{{ source('public', 'vidyard_players') }} as vidyard_players