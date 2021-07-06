select
	a.accountid
    , case 
    	when (p.sku LIKE 'SS-010' AND s.status LIKE 'Active') then 'active_pro'
        when (p.sku LIKE 'SS-010' AND s.status LIKE 'Cancelled' AND s.termenddate >= getdate()) then 'active_pro'
        else '--Not yet classified--'
      end as user_type
    , a.accountname
    , a.accountnumber
    , a.accountstatus
    , a.billtocontactid
    , a.createdbyid
    , a.createddate
    , a.crmid
    , a.customeracquisitiondate
    , a.net_mrr
    , a.vidyardid
    , s.cancelleddate
    , s.contractstartdate
    , s.currentterm
    , s.currenttermperiodtype
    , s.defaultpaymentmethodid
    , s.initialterm
    , s.initialtermperiodtype
    , s.name as susbcriptionname
    , s.originalcreateddate
    , s.originalsubscriptionid
    , s.previoussubscriptionid
    , s.renewalterm
    , s.renewaltermperiodtype
    , s.soldtocontactid
    , s.status
    , s.subscriptionenddate
    , s.subscriptionid
    , s.subscriptionstartdate
    , s.subscriptionversionamendmentid as ssubscriptionversionamendmentid
    , s.termenddate
    , s.termstartdate
    , s.termtype
    , s.updatedbyid as subscriptionupdatedby
    , s.updateddate as subscriptionupdateddate
    , s.vidyardcanceldate
    , prp.activecurrencies
    , prp.billingperiod
    , prp.description
    , prp.effectiveenddate
    , prp.effectivestartdate
    , prp.name as productrateplanname
    , prp.productid
    , p.sku
    , prp.productrateplanid
    , prp.pvstatus
    , prp.updatedbyid as productrateplanupdatedby
    , prp.updateddate productrateplanupdateddate
    , rp.amendmentid
    , rp.amendmentsubscriptionrateplanid
    , rp.amendmenttype
    , rp.subscriptionversionamendmentid as rpsubscriptionversionamendmentid
    , rp.triggersync
from {{ ref('stg_zuora_rate_plan') }} as rp 
        join {{ ref('stg_zuora_subscription') }} as s on s.subscriptionid = rp.subscriptionid
        join {{ ref('stg_zuora_account') }} as a on a.accountid = s.accountid
        join {{ ref('stg_zuora_product_rate_plan') }} as prp on prp.productrateplanid = rp.productrateplanid
        join {{ ref('stg_zuora_product') }} as p on p.productid = prp.productid
        join {{ ref('stg_zuora_contact') }} c on c.contactid = s.soldtocontactid