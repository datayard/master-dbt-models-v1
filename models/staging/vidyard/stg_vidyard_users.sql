SELECT 
	vidyard_users.id as userId,
	vidyard_users.email as email,
	vidyard_users.current_sign_in_at,
	vidyard_users.created_at as createdDate,
	vidyard_users.updated_at as updatedDate,
	vidyard_users.first_name as firstName,
	vidyard_users.last_name as lastName
 FROM 
	{{ source('public', 'vidyard_users') }} as vidyard_users