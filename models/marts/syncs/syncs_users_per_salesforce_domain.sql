WITH usertype AS

(
SELECT 
usrdetails.domain,
COUNT (CASE WHEN usrdetails.personal_account_type like 'free' then 1 end) as free,
COUNT (CASE WHEN usrdetails.personal_account_type like 'pro' then 1 end) as pro,
COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise' then 1 end) as enterprise,
COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise self serve' then 1 end) AS enterprise_self_serve
  
FROM 
{{ ref('tier2_vidyard_user_details') }} usrdetails
  
GROUP BY
usrdetails.domain
  ),


sfdc AS

(
SELECT
sfdcaccount.accountid, 
sfdcaccount.emaildomain

FROM
{{ref('tier2_salesforce_account')}} sfdcaccount

WHERE
sfdcaccount.ispersonaccount = FALSE
)

SELECT
users.domain,
usertype.free,
usertype.pro,
usertype.enterprise,
--sfdc.accountid,
(CASE WHEN sfdc.accountid is NULL then 'NEW' end) as newaccount

FROM
{{ ref('tier2_vidyard_users')}}  users

LEFT JOIN
usertype

ON 
users.domain = usertype.domain

LEFT JOIN
sfdc

ON 
users.domain = sfdc.emaildomain

where users.domain NOT LIKE '%.edu' AND
users.domain NOT LIKE '%.ru' AND
users.domain NOT LIKE '%.br' AND
users.domain NOT LIKE '%.se' AND
users.domain NOT LIKE '%.cl' AND
users.domain NOT LIKE '%.in' AND
users.domain NOT LIKE '%.my' AND
users.domain NOT LIKE '%.mx' AND

users.domain not in ( Select emaildomain From dbt_vidyard_master.stg_free_domains) AND
sfdc.accountid is NULL

--WHERE  sfdc.accountid is NULL


group by 1,2,3,4,5