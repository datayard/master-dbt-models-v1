SELECT
    vyorgs.accountid,
    vyorgs.organizationid,
    CASE WHEN vyorgs.organizationid IN (SELECT organizationid FROM {{ ref('stg_vidyard_active_features') }} WHERE featureid = 54) THEN true ELSE false end as SSOenabled,
    CASE WHEN vyorgs.organizationid IN (SELECT organizationid FROM {{ ref('stg_vidyard_active_features') }} WHERE featureid = 106) THEN true ELSE false end as GDPRenabled,
    CASE WHEN vyorgs.organizationid IN (SELECT organizationid FROM {{ ref('stg_vidyard_active_features') }} WHERE featureid = 107) THEN true ELSE false end as GDPRrequestenabled,
    CASE WHEN vyorgs.organizationid IN (SELECT organizationid FROM {{ ref('stg_vidyard_active_features') }} WHERE featureid = 73) THEN true ELSE false end as SEOenabled
FROM
    --dbt_vidyard_master.stg_vidyard_organizations as vyorgs
    {{ ref('stg_vidyard_organizations') }} vyorgs
WHERE
    true

