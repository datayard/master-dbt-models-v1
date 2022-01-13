    SELECT
        vy_orgs.accountId,
        vy_orgs.organizationID,
        vy_active_f.featureid,
        vy_active_f.activefeatureid,
        vy_active_f.createddate
    FROM
        {{ ref('stg_vidyard_organizations') }} as vy_orgs
    LEFT JOIN
        {{ ref('stg_vidyard_active_features') }} as vy_active_f
    ON
        vy_orgs.organizationID = vy_active_f.organizationId
    WHERE
        vy_active_f.featureid = 49