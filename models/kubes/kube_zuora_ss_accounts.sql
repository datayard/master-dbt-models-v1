with accounts 
    as (
    select
        accountid
        , count(distinct subscriptionid) as subscriptioncount
        , count(distinct case when subscriptionenddate is null or subscriptionenddate >= getdate() then subscriptionid end) as activesubscriptioncount
        , sum(netmrr) as accountmrr

    from 
        --dbt_vidyard_master.kube_zuora_ss_subscriptions
        {{ ref('kube_zuora_ss_subscriptions') }}
    group by 1
    )

, accountscase1 
    as (
    select
        accounts.accountid
        , subscriptioncount
        , activesubscriptioncount
        , accountmrr
        , sub.subscriptionid
    from 
        accounts
        --join dbt_vidyard_master.kube_zuora_ss_subscriptions sub on sub.accountid = accounts.accountid
        join {{ ref('kube_zuora_ss_subscriptions') }} sub on sub.accountid = accounts.accountid
    where 
        subscriptioncount = 1
)
, accountscase2 
    as (
    select
        accounts.accountid
        , subscriptioncount
        , activesubscriptioncount
        , accountmrr
        , sub.subscriptionid
    from 
        accounts
        --join dbt_vidyard_master.kube_zuora_ss_subscriptions sub on sub.accountid = accounts.accountid
        join {{ ref('kube_zuora_ss_subscriptions') }} sub on sub.accountid = accounts.accountid
    where 
        subscriptioncount != 1 
        and activesubscriptioncount = 1
        and (subscriptionenddate is null or subscriptionenddate >= getdate())

)

, accountscase3 
    as (
    select
        accounts.accountid
        , subscriptioncount
        , activesubscriptioncount
        , accountmrr
        , sub.subscriptionid
        , RANK () OVER ( PARTITION BY sub.accountid ORDER BY subscriptionstartdate DESC) as subrank
    from accounts
        --join dbt_vidyard_master.kube_zuora_ss_subscriptions sub on sub.accountid = accounts.accountid
        join {{ ref('kube_zuora_ss_subscriptions') }} sub on sub.accountid = accounts.accountid
    where
        activesubscriptioncount >1
        and (subscriptionenddate is null or subscriptionenddate >= getdate())

)
, allaccounts
    as (
    select
        *
    from
        accountscase1
    union
    select
        *
    from
        accountscase1
    union
    select
        accountid
        , subscriptioncount
        , activesubscriptioncount
        , accountmrr
        , subscriptionid
    from
        accountscase3
    where
        subrank = 1
    )

select
    allaccounts.accountid
    , vidyardaccountid
    , subscriptioncount
    , activesubscriptioncount
    , accountmrr
    , allaccounts.subscriptionid as currentsubscriptionid
    , productrateplanname as currentproductrateplanname
    , sku as cuZrrentsku
    , subscriptionstartdate as currentsubscriptionstartdate
    , subscriptionenddate as currentsubscriptionenddate
    , freetrialstart as currentfreetrialstart
    , freetrialend as currentfreetrialend
    , initialtrial as currentinitialtrial
    , daysleftontrial as currentdaysleftontrial
    , sublongerthenoneday as currentsublongerthenoneday
    , promo as currentpromo
    , promostartdate as currentpromostartdate
    , promoenddate as currentpromoenddate
    , daysleftonpromo as currentdaysleftonpromo
    , subtype as currentsubtype
    , trailbillthrough as currenttrailbillthrough
    , trailbillthroughnextday as currenttrailbillthroughnextday
    , promocodebillthrough as currentpromocodebillthrough
    , billthroughdate as currentbillthroughdate
from allaccounts
--join dbt_vidyard_master.kube_zuora_ss_subscriptions sub on sub.accountid = allaccounts.accountid
join {{ ref('kube_zuora_ss_subscriptions') }} sub on sub.accountid = allaccounts.accountid