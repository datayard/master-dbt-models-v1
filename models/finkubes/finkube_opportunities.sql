   select
     opportunityid
     , accountid
     , opportunitytype
     , stagename
      , opportunityattribution
      , c.region
     , to_char(date_trunc('month',case when enteredPipelineDate is null and (stagename like '%Closed%' or (opportunitytype in ('Renewals','Renewals + Add-On'))) then nvl(closedwondate,closedate) else enteredPipelineDate end),'yyyy-mm') as pipelinemonth
     , case when (case when stagename like '%Dead%' then to_char(date_trunc('month',deaddate),'yyyy-mm')
          else nvl(to_char(date_trunc('month',closedwondate),'yyyy-mm'),to_char(date_trunc('month',closedate),'yyyy-mm')) end) <= pipelinemonth then pipelinemonth else
          (case when stagename like '%Dead%' then to_char(date_trunc('month',deaddate),'yyyy-mm')
               else nvl(to_char(date_trunc('month',closedwondate),'yyyy-mm'),to_char(date_trunc('month',closedate),'yyyy-mm')) end)  end as exitmonth

     , nvl(lastyeararr,0) as previousarr

     , case when opportunitytype in ('Renewals', 'Renewals + Add-On') then previousarr else 0 end as renewalpipelinearr
     , case when opportunitytype not in ('New Business')
          then nvl(newarr,0) + (case when nvl(renewalamount,0) > previousarr then nvl(renewalamount,0) - previousarr else 0 end)
          else 0 end as upsellpipelinearr
     , case when opportunitytype in ('New Business') then nvl(newarr,0) else 0 end as newpipelinearr
     , renewalpipelinearr + upsellpipelinearr + newpipelinearr as pipelinearr


     , case when opportunitytype in ('Renewals', 'Renewals + Add-On') and (stagename like '%Dead%' or stagename like '%Closed%') then
             previousarr + nvl(renewallostarr,0) else 0 end as renewalclosedarr
     , case when opportunitytype not in ('New Business') and stagename like '%Closed%'
            then nvl(newarr,0) + (case when renewalamount > previousarr then nvl(renewalamount,0) - previousarr else 0 end)
            else 0 end as upsellclosedarr
     , case when opportunitytype = 'New Business' and stagename like '%Closed%' then nvl(newarr,0) else 0 end as newclosedarr
     , renewalclosedarr + upsellclosedarr + newclosedarr as closedarr


   from
     {{ref('tier2_salesforce_opportunity')}}
   left join
     {{ref('tier2_salesforce_account')}} a using (accountid)
   left join {{ref('fct_sfdc_country_to_region')}} c on a.billingcountry = c.country
    where a.ispersonaccount = 'false' and (closedarr <> 0 or pipelinearr <> 0) and pipelinemonth is not null
