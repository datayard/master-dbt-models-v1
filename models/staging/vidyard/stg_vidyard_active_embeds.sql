SELECT 
	vidyard_active_embeds.id as embedID,
	vidyard_active_embeds.account_id as accountID

 FROM 
	{{ source('public', 'vidyard_active_embeds') }} as vidyard_active_embeds