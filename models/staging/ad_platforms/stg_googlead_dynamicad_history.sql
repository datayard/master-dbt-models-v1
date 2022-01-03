SELECT
    dyhistory.ad_group_id as adgroupID,
    dyhistory.ad_id as adID,
    dyhistory.description as description1,
    dyhistory.description_2 as description2
    
FROM 
	{{ source('google_adwords', 'expanded_dynamic_search_ad_history') }} as dyhistory