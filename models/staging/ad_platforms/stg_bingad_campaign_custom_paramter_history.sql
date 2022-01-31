SELECT
    cp.campaign_id as campaignId,
    cp.key as parameterKey,
    cp.value as parameterValue
    
FROM 
	{{ source('bingads', 'campaign_custom_parameter_history') }} as cp