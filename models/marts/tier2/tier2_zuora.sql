SELECT a.accountid
         , CASE
                WHEN (p.sku LIKE 'SS-010' AND s.status LIKE 'Active') THEN 'Active - Pro'
                WHEN (p.sku LIKE 'SS-010' AND s.status LIKE 'Cancelled' AND s.termenddate >= getdate()) THEN 'Active - Pro'
                WHEN (p.sku LIKE 'SS-010' AND s.status LIKE 'Cancelled' AND s.termenddate < getdate()) THEN 'Cancelled - Pro'
                WHEN (prp.productrateplanid = '2c92a010786db3e001786fe1669267ad' AND s.status LIKE 'Active') THEN 'Active - Self Serve Enterprise'
                WHEN (prp.productrateplanid = '2c92a010786db3e001786fe1669267ad' AND s.status LIKE 'Cancelled' AND s.termenddate >= getdate()) THEN 'Active - Self Serve Enterprise'
                WHEN (prp.productrateplanid = '2c92a010786db3e001786fe1669267ad' AND s.status LIKE 'Cancelled' AND s.termenddate < getdate()) THEN 'Cancelled - Self Serve Enterprise'
                WHEN (s.status LIKE 'Active') THEN 'Active - Others'
                WHEN (s.status LIKE 'Cancelled' AND s.termenddate >= getdate()) THEN 'Active - Others'
                WHEN (s.status LIKE 'Cancelled' AND s.termenddate < getdate()) THEN 'Cancelled - Others'
                ELSE '--Not yet classified--'
           END AS subscription_type
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
         , s.name AS susbcriptionname
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
         , s.subscriptionversionamendmentid AS subscriptionversionamendmentid
         , s.termenddate
         , s.termstartdate
         , s.termtype
         , s.updatedbyid AS subscriptionupdatedby
         , s.updateddate AS subscriptionupdateddate
         , s.vidyardcanceldate
         , prp.activecurrencies
         , prp.billingperiod
         , prp.description
         , prp.effectiveenddate
         , prp.effectivestartdate
         , prp.name AS productrateplanname
         , prp.productid
         , p.sku
         , prp.productrateplanid
         , prp.pvstatus
         , prp.updatedbyid AS productrateplanupdatedby
         , prp.updateddate AS productrateplanupdateddate
         , rp.rateplanid
         , rp.amendmentid
         , rp.amendmentsubscriptionrateplanid
         , rp.amendmenttype
         , rp.subscriptionversionamendmentid AS rpsubscriptionversionamendmentid
         , rp.triggersync
    FROM {{ ref('stg_zuora_rate_plan') }} AS rp
             JOIN {{ ref('stg_zuora_subscription') }} AS s ON s.subscriptionid = rp.subscriptionid
             JOIN {{ ref('stg_zuora_account') }} AS a ON a.accountid = s.accountid
             JOIN {{ ref('stg_zuora_product_rate_plan') }} AS prp ON prp.productrateplanid = rp.productrateplanid
             JOIN {{ ref('stg_zuora_product') }} AS p ON p.productid = prp.productid
             JOIN {{ ref('stg_zuora_contact') }} c ON c.contactid = s.soldtocontactid