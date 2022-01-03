SELECT
    kwstat.date as date,
    kwstat.ad_network_type_1 as networkType1,
    kwstat.ad_network_type_2 as networkType2,
    kwstat.cost as cost,
    kwstat.campaign_id as campaignID,
    kwstat.impressions as impressions,
    kwstat.device as device,
    kwstat.conversions as conversions,
    kwstat.slot as kwSlot,
    kwstat.id as kwID,
    kwstat.interactions as interactions,
    kwstat.ad_group_id as adgroupID,
    kwstat.clicks as clicks

FROM 
	{{ source('google_adwords', 'keyword_stats') }} as kwstat
WHERE
    kwstat.date >= '2020-01-01'