WITH mktoPerson AS  -- Marketo Persons
(
SELECT
    DISTINCT mktoLeads.leadid,
    substring (mktoLeads.emailaddress, charindex( '@', mktoLeads.emailaddress) + 1,
    len(mktoLeads.emailaddress)) AS Domain
FROM
     {{ ref('stg_marketo_lead') }} mktoLeads
WHERE
    mktoLeads.leadid NOT IN (SELECT merged_lead FROM  {{ source('marketo', 'merged_lead') }} ) --AND
)

SELECT
    count(DISTINCT mktoPerson.leadid) as marketoLeads,
    domain
FROM
    mktoPerson
GROUP BY
    domain