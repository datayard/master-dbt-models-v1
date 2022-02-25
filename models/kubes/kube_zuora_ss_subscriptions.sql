with all_subs
    as (
    select 
        distinct
        accountid
        , accountnumber
        , vidyardaccountid
        , subscriptionid
        , productrateplanname
        , sku
        , subscriptionstartdate
        , subscriptionenddate
        , mrr
        , discountpercentage
        , termenddate
    from 
        {{ ref('tier2_zuora') }}
    where
        sku in ('SKU-00000009', 'SKU-00000020', 'SS-010')
        and status != 'Expired'
        and (amendmenttype is null or amendmenttype != 'RemoveProduct')
        and discountpercentage is null
        and mrr != 0
        and productrateplanname in ('Vidyard Pro Monthly', 'Vidyard Pro Annual', 'Annual','Advisor Group Monthly', 'Advisor Group Annual')
    )

select
    s.accountid
    , vidyardaccountid
    ,s.subscriptionid
    , accountnumber
    ,s.productrateplanname
    , termenddate
    , s.sku
    , s.subscriptionstartdate
    , s.subscriptionenddate
    , s.mrr
    , ft.freetrialstart
    , ft.freetrialend
    , ft.initialtrial
    , ft.daysleftontrial,
    (subscriptionenddate != subscriptionstartdate) or subscriptionenddate is null as sublongerthenoneday
    , p.promo
    , p.promocode
    , p.promostartdate
    , p.promoenddate
    , p.daysleftonpromo
    , case
        when promoEndDate is not null and freetrialend is not null then 'Free Trial and Limited Promo Code'
        when promoEndDate is not null then 'Limited Promo'
        when promoStartDate is not null then 'Forever Promo'
        when freetrialend is not null then 'Free Trial'
        else 'Standard Subscription'
        end as subtype
    , case
        when subtype in ('Free Trial and Limited Promo Code', 'Free Trial')  then getdate() >= freetrialend and (s.subscriptionenddate > freetrialend or s.subscriptionenddate is null)
        else true
        end as trailbillthrough
    , case
        when subtype in ('Free Trial and Limited Promo Code', 'Free Trial')  then getdate() >= freetrialend and (s.subscriptionenddate > dateadd(day, 1, freetrialend) or s.subscriptionenddate is null)
        else true
        end as trailbillthroughnextday
    , case
        when subtype in ('Free Trial and Limited Promo Code', 'Limited Promo') then getdate() >= promoenddate and (s.subscriptionenddate > promoenddate or s.subscriptionenddate is null)
        else true
        end as promocodebillthrough
    , case
        when subtype = 'Free Trial and Limited Promo Code' and promoenddate > freetrialend then promoenddate
        when subtype = 'Free Trial and Limted Promo Code' then freetrialend
        when subtype = 'Limited Promo' then promoenddate
        when subtype = 'Free Trial' then freetrialend
        else s.subscriptionstartdate
        end as billthroughdate
    , case
        when subtype = 'Forever Promo' then mrr * (p.discountpercentage/100) else 0
        end as discount_amount,
    mrr - discount_amount as netmrr
from 
    all_subs s
    --left join free_trials ft on s.subscriptionid = ft.subscriptionid
    left join {{ ref('tier2_free_trials') }} as ft on s.subscriptionid = ft.subscriptionid
    --left join promos p on p.subscriptionid = s.subscriptionid 
    left join {{ ref('tier2_all_promo_codes') }} as p on p.subscriptionid = s.subscriptionid 