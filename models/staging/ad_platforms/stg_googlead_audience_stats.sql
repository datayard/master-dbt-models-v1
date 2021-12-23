SELECT
    austat.date as date,
    austat.ad_network_type_1 as networkType1,
    austat.ad_network_type_2 as networkType2,
    austat.cost as cost,
    austat.campaign_id as campaignID,
    austat.impressions as impressions,
    austat.slot as audienceSlot,
    austat.id as audienceID,
    austat.interactions as interactions,
    austat.ad_group_id as adgroupID,
    austat.clicks as clicks

FROM 
	{{ source('google_adwords', 'audience_stats') }} as austat
WHERE
    austat.date >= '2020-01-01'