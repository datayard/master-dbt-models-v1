WITH userType AS  -- User types

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


mktoPerson AS  -- Marketo Persons
(
SELECT
    DISTINCT mktoLeads.leadid,
    substring (mktoLeads.emailaddress, charindex( '@', mktoLeads.emailaddress) + 1,
    len(mktoLeads.emailaddress)) AS Domain
FROM
     {{ ref('stg_marketo_lead') }} mktoLeads
WHERE
    mktoLeads.leadid NOT IN (SELECT merged_lead FROM  {{ source('marketo', 'merged_lead') }} ) --AND
),


mktoPersonDomain AS -- Marketo persons grouped by domains
(
    SELECT
    count(DISTINCT mktoPerson.leadid) as marketoLeads,
    domain
FROM
    mktoPerson
GROUP BY
    domain
),


sfdcContactsDomain AS  --SFDC contacts grouped by domains
(

SELECT
    count(distinct contactid) AS sfdcContacts, domain
FROM
    {{ ref('stg_salesforce_contact') }}
GROUP BY
    domain
),


sfdcLeadsAttachedDomain AS   -- SFDC leads attached to a company
(
SELECT
    count (distinct leadid) as sfLeadsAttached, domain
FROM
     {{ ref('stg_salesforce_lead') }}
WHERE
      isconverted is false AND
      isdeleted is false AND
      company is NOT NULL AND
      domain NOT IN (SELECT emaildomain from {{ ref('stg_free_domains') }} )
GROUP BY
         domain
),


sfdcLeadsUnattachedDomain AS (

SELECT
    count(distinct leadid) AS SfLeadsUnattached, domain
FROM
     {{ ref('stg_salesforce_lead') }}
WHERE
      isconverted is false AND
      isdeleted is false AND
      company is NULL AND
      domain NOT IN (SELECT emaildomain from {{ ref('stg_free_domains') }} )
GROUP BY
         domain
    ),


forceCreate AS              -- Force create when a lead is member of marketo Madison Logic list
(
    SELECT
        domain,
        (CASE WHEN mktoPerson.leadid IN (
        SELECT
            id
        FROM
             {{ source('marketo', 'list_membership') }}
        WHERE
            list_id = 18072
        ) then 1 else 0
        end ) as forceCreatefromCampaign
    FROM
        mktoPerson
    where forceCreatefromCampaign = 1
    ),


sfdc AS                 -- SFDC Account domain
(
SELECT
    sfdcaccount.accountid,
    sfdcaccount.emaildomain

FROM
    {{ ref('tier2_salesforce_account') }} sfdcaccount 
WHERE
    sfdcaccount.ispersonaccount = FALSE
)


SELECT
    users.domain,
    userType.free,
    userType.pro,
    userType.enterprise,
    mktoPersonDomain.marketoLeads,
    sfdcContactsDomain.sfdcContacts,
    sfdcLeadsAttachedDomain.sfLeadsAttached,
    sfdcLeadsUnattachedDomain.SfLeadsUnattached,
    --sfdc.accountid
    (CASE WHEN sfdc.accountid is NULL then 'null' else sfdc.accountid end) as sfdcAccountID,
    forceCreate.forceCreatefromCampaign

FROM
    {{ ref('tier2_vidyard_users') }} users 

LEFT JOIN
    userType
ON
    users.domain = usertype.domain

FULL JOIN
    mktoPersonDomain
ON
    users.domain = mktoPersonDomain.domain

FULL JOIN
    sfdcContactsDomain
ON
    users.domain = sfdcContactsDomain.domain

FULL JOIN
    sfdcLeadsAttachedDomain
ON
    users.domain = sfdcLeadsAttachedDomain.domain

FULL JOIN
    sfdcLeadsUnattachedDomain
ON
    users.domain = sfdcLeadsUnattachedDomain.domain

FULL JOIN
    sfdc
ON
    users.domain = sfdc.emaildomain

FULL JOIN
    forceCreate
ON
    users.domain = forceCreate.Domain

WHERE
    users.domain NOT LIKE '%.edu' AND
    users.domain NOT LIKE '%.ru' AND
    users.domain NOT LIKE '%.br' AND
    users.domain NOT LIKE '%.se' AND
    users.domain NOT LIKE '%.cl' AND
    users.domain NOT LIKE '%.in' AND
    users.domain NOT LIKE '%.my' AND
    users.domain NOT LIKE '%.mx' AND
    users.domain not in ( Select emaildomain From dbt_vidyard_master.stg_free_domains)
    --forceCreate.forceCreatefromCampaign is NOT NULL

--WHERE  sfdc.accountid is NULL


GROUP BY
         1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10