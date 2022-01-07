WITH 

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
    )

SELECT
    users.domain,
    users.free,
    users.pro,
    users.enterprise,
    mktoperson.leads,
    sfdcContactsDomain.sfdcContacts,
    sfdcLeadsAttachedDomain.sfLeadsAttached,
    sfdcLeadsUnattachedDomain.SfLeadsUnattached,
    (CASE WHEN sfdcdomains.accountid is NULL then NULL else sfdcdomains.accountid end) as sfdcAccountID,
    (CASE WHEN users.domain in (SELECT domain from {{ ref('tier2_marketo_madisonlogiclist') }}) THEN 1 ELSE 0 end) as forceCreatefromCampaign
FROM
    {{ ref('tier2_users_per_domain') }} users 
FULL JOIN
     {{ ref('tier2_marketo_persons') }} mktoperson 
ON
    users.domain = mktoperson.domain
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
    {{ ref('tier2_salesforce_domain_matching') }} sfdcdomains
ON
    users.domain = sfdcdomains.emaildomain
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