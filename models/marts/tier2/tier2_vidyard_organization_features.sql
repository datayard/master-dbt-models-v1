WITH nve_activated as (
    SELECT 
        vy_orgs.accountId,
        vy_orgs.organizationID, 
    CASE
        WHEN vy_active_f.featureid = '134' THEN true ELSE false END as nve_activated,
        vy_active_f.createddate as nveactivateddate
    FROM  
        {{ ref('stg_vidyard_organizations') }} as vy_orgs
    LEFT JOIN
        {{ ref('stg_vidyard_active_features') }} as vy_active_f
    ON
        vy_orgs.organizationID = vy_active_f.organizationId
    WHERE 
        vy_active_f.featureid = '134'
    ) 
    
SELECT
    *
FROM
    nve_activated
WHERE
    true
