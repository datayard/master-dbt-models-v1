SELECT
    aghistory.id as adgroupID,
    aghistory.name as adgroupName,
    aghistory.status as adgroupStatus,
    aghistory.ad_group_type as adgroupType,
    aghistory.campaign_name as campaignName,
    aghistory.campaign_id as campaignID
    
FROM 
	{{ source('google_adwords', 'ad_group_history') }} as aghistory