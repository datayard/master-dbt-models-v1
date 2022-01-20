SELECT
    cp.campaign_id as campaignId,
    cp.key as parameterKey,
    cp.value as parameterValue,
    cp.value as testcolumn,
FROM 
	{{ source('bingads', 'campaign_custom_parameter_history') }} as cp