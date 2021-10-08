with pipeline as (
   select *, 1 as connector from {{ref('finkube_pipeline')}}
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
      where exitmonth = yearmonth
  )

  , lost as (
    SELECT
     *, 'lost' as changetype
     from pipeline
     where exitmonth = yearmonth
  )

  select * from gross_new union select * from won union select * from lost
