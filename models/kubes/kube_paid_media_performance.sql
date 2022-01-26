with 
    campaignad_performance as (
          select 
            gs.date as date
            , cast(gs.campaignID as char(256)) as campaignAdId 
            -- , gh.campaignName as campaignAdName
            , sum(gs.cost) as spend
            , sum(gs.impressions) as impressions
            , sum(gs.clicks) as clicks
            -- , gs.conversions

          from {{ ref('stg_googlead_campaign_hourlystats')}} as gs
            join {{ ref('stg_googlead_campaign_history')}} as gh
              on gs.campaignID = gh.campaignId
          group by 1,2

          union all

          select
            bs.date as date
            , cast(bs.campaignId as char(256)) as campaignAdId 
            -- , bh.campaignName as campaignAdName
            , sum(bs.dailySpend) as spend
            , sum(bs.dailyImpressions) as impressions
            , sum(bs.dailyClicks) as clicks
            -- , bs.dailyConversions

          from {{ ref('stg_bingad_campaign_daily_performance')}} as bs
            join {{ ref('stg_bingad_campaign_history')}} as bh
              on bs.campaignId = bh.campaignId
          group by 1,2

          union all 

          select
            fs.date as date
            , fs.adId as campaignAdId
            -- , ad_name as campaignAdName
            , sum(dailySpend) as spend
            , sum(dailyImpressions) as impressions
            , sum(cast(round(dailySpend*1/nullif(dailyCpc,0)) as bigint)) as clicks

          from {{ ref('stg_facebook_report')}} as fs
          group by 1,2
    ),

    campaign_performance_summary as (
          select 
            cp.date
            , cr.utm_campaign
            , cr.utm_source
            , cr.utm_medium
            , sum(cp.spend) as totalDailySpend
            , sum(cp.impressions) as totalDailyImpressions
            , sum(cp.clicks) as totalDailyClicks
          from campaignad_performance as cp
            join {{ ref('tier_2_campaign_reference')}} cr
              on cp.campaignAdId = cr.campaignAdId
          group by
              1,
              2,
              3,
              4
    ),

    vidyard_users as (
          select
            cast(vu.createddate as date) as date
            , heap.utmcampaign as utmCampaign 
            , heap.utmsource as utmSource
            , heap.utmmedium as utmMedium 
            , count(distinct case when (date_diff('minute', heap.eventtime, vu.createddate) between 0 and 30) then vu.organizationid end) as signups
            , count(distinct case when (date_diff('minute', heap.eventtime, vu.createddate) between 0 and 30) and (vu.domaintype = 'business') then vu.organizationid end) as buSignups
            , count(distinct case when (date_diff('minute', heap.eventtime, vu.createddate) between 0 and 30) and (vu.domaintype = 'business') and (vu.combined_usecase = 'marketing') then vu.organizationid end) as buMarketing
            , count(distinct case when (date_diff('minute', heap.eventtime, vu.createddate) between 0 and 30) and (vu.domaintype = 'business') and (vu.combined_usecase = 'sales') then vu.organizationid end) as buSales
            , count(distinct case when (date_diff('minute', heap.eventtime, vu.createddate) between 0 and 30) and (vu.domaintype = 'business') and (vu.activatedflag = 1) then vu.organizationid end) as buActivations

          from {{ ref('tier2_heap')}} as heap
            left join {{ ref('kube_vidyard_user_details')}} as vu 
              on heap.vidyardUserId = vu.userid 
                  and vu.excludeemail = 0 
                  and vu.signupsource != 'Hubspot' 
                  and vu.classification in ('free','pro')
            where 
              heap.channels = 'Paid' 
                and heap.tracker = 'vy_com_page_view'
                and (vu.createddate >= timestamp '2020-12-01' and vu.createddate < timestamp '2021-12-31')
            group by 
                1,
                2,
                3,
                4  
    )

select 
  vu.date
  , vu.utmCampaign
  , vu.utmSource
  , vu.utmMedium
  , cps.totalDailySpend
  , cps.totalDailyImpressions
  , cps.totalDailyClicks
  , vu.signups
  , vu.buSignups
  , vu.buMarketing
  , vu.buSales
  , vu.buActivations

from campaign_performance_summary as cps
  join vidyard_users as vu
     on cps.date = vu.date and cps.utm_campaign = vu.utmCampaign and cps.utm_source = vu.utmSource and cps.utm_medium = vu.utmMedium




