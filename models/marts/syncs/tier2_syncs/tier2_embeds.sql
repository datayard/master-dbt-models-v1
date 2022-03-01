WITH active_embeds as (
    SELECT
        ae.accountId,
        COUNT (distinct ae.embedId) as activeembeds
    FROM
        {{ ref('tier2_vidyard_active_embeds') }} as ae
    WHERE
        true
    GROUP BY
        ae.accountId
),

embedlimits as (
    SELECT  
        *
    FROM
        {{ ref('stg_vidyard_allotment_limits') }} as vyalot
    WHERE   
        vyalot.allotmentTypeID = '3'
)

SELECT  
    vyorgs.organizationid,
    vyorgs.accountId,
    ae.activeembeds,
    embedlimits.allotmentlimit,
    embedlimits.gracelimit,
    CASE 
        WHEN embedlimits.allotmentlimit = -1 then -1
        WHEN embedlimits.allotmentlimit != -1 then ((embedlimits.allotmentlimit + embedlimits.gracelimit)-ae.activeembeds) 
        else null
        END as remaininembeds,
    embedlimits.enforced
FROM    
    {{ ref('stg_vidyard_organizations') }} as vyorgs
LEFT JOIN
    active_embeds as ae
ON
    vyorgs.accountId = ae.accountId
LEFT JOIN
    embedlimits
ON
    vyorgs.accountId = embedlimits.accountID