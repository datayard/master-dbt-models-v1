select distinct 
    emaildomain
    , z.vidyardaccountid
    , z.accountname
    , sf.accountid
    , accountstatus = 'Active' as activecustomer
from
-- dbt_vidyard_master.tier2_zuora z
{{ ref('tier2_zuora') }} z
-- left join dbt_vidyard_master.tier2_salesforce_account sf 
lEFT JOIN {{ ref('tier2_salesforce_account') }} sf
    on z.vidyardaccountid = sf.vidyardaccountid
where
    z.sku not in ('SS-010', 'SKU-00000009', 'SKU-00000020')
    and z.customeracquisitiondate >= '2019-05-01'
    and z.customeracquisitiondate < '2021-05-01'