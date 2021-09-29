with dates_table as (
  select
    yearmonthvalue
    , yearmonth
    , fiscalyearmonth
    , fiscalperiod
    , fiscalyear
    , 1 as connector
 from {{ ref('fct_sfdc_dates') }}
 group by 1,2,3,4,5
)


, pipeline_no_time as (
   select * from {{ref('finkube_opportunities')}}
 )

   select
     accountid
     , opportunityid
     , opportunityattribution
     , opportunitytype
     , region

     , pipelinemonth
     , exitmonth
     , yearmonth
     , yearmonthvalue
     , yearmonthvalue - left(pipelinemonth, 4) * 12 - right(pipelinemonth, 2) * 1 as age
     , fiscalyearmonth
     , fiscalperiod
     , fiscalyear

     , pipelinearr
     , newpipelinearr
     , renewalpipelinearr
     , upsellpipelinearr

  --   , closedarr
  --   , newclosedarr
  --   , renewalclosedarr
  --   , upsellclosedarr
  --  , previousarr

   from
     pipeline_no_time
       left join dates_table on
         yearmonth >= pipelinemonth
         and yearmonth <= exitmonth
     where
       yearmonth is not null
