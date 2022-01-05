SELECT
    --listmembership.list_id as listid,
    COUNT (distinct mktoleads.leadid) as leadid,
    substring (mktoLeads.emailaddress, charindex( '@', mktoLeads.emailaddress) + 1,
    len(mktoLeads.emailaddress)) AS Domain
FROM
    {{ source('marketo', 'list_membership') }} as listmembership
JOIN
    {{ ref('stg_marketo_lead') }}  as mktoleads 
ON
    listmembership.id = mktoleads.leadid
WHERE
    list_id = 18072
GROUP BY
    domain