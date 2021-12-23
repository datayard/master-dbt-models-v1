SELECT
    adstat.date as date,
    adstat.ad_network_type_1 as networkType1,
    adstat.ad_network_type_2 as networkType2,
    adstat.cost as cost,
    adstat.campaign_id as campaignID,
    adstat.impressions as impressions,
    adstat.device as device,
    adstat.conversions as conversions,
    adstat.slot as adSlot,
    adstat.id as adID,
    adstat.ad_group_id as adgroupID,
    adstat.clicks as clicks

FROM 
	{{ source('google_adwords', 'ad_stats') }} as adstat
WHERE
    adstat.date >= '2020-01-01'