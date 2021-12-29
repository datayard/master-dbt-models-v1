SELECT
    adhistory.ad_group_id as adgroupID,
    adhistory.id as adID,
    adhistory.ad_type as adType,
    adhistory.status as adStatus

FROM 
	{{ source('google_adwords', 'ad_history') }} as adhistory