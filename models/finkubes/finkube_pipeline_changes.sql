with pipeline as (
   select * from {{ref('finkube_pipeline')}}
 )

, gross_new as (
  select
    *, 'gross new' as changetype
  from pipeline
  where pipelinemonth = yearmonth
  )

  , won as (
    select
      *, 'won' as changetype
      from pipeline
      where exitmonth = yearmonth and closedarr > 0
  )

  , lost as (
    SELECT
     *, 'lost' as changetype
     from pipeline
     where exitmonth = yearmonth and closedarr <= 0
  )

  select
    accountid
    , opportunityid
    , opportunityattribution
    , opportunitytype
    , customertype
    , region
    , contractlengthtype
    , pipelinemonth
    , exitmonth
    , yearmonth
    , yearmonthvalue
    , age
    , fiscalyearmonth
    , fiscalquarter
    , pipelinearr
    , newpipelinearr
    , renewalpipelinearr
    , upsellpipelinearr
    , changetype
  from gross_new

  union all

select
  accountid
  , opportunityid
  , opportunityattribution
  , opportunitytype
  , customertype
  , region
  , contractlengthtype
  , pipelinemonth
  , exitmonth
  , yearmonth
  , yearmonthvalue
  , age
  , fiscalyearmonth
  , fiscalquarter
  , pipelinearr
  , newpipelinearr
  , renewalpipelinearr
  , upsellpipelinearr
  , changetype
from won

  union all

select
   accountid
   , opportunityid
   , opportunityattribution
   , opportunitytype
   , customertype
   , region
   , contractlengthtype
   , pipelinemonth
   , exitmonth
   , yearmonth
   , yearmonthvalue
   , age
   , fiscalyearmonth
   , fiscalquarter
   , pipelinearr
   , newpipelinearr
   , renewalpipelinearr
   , upsellpipelinearr
   , changetype
 from lost
