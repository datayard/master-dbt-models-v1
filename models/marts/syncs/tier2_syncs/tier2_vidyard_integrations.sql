SELECT
    orgs.accountid,
    listagg (distinct tokens.token_type,';') as integration
FROM
    {{ ref('stg_vidyard_api_tokens') }} as tokens
JOIN
    {{ ref('stg_vidyard_organizations') }} as orgs
ON
    tokens.organizationid = orgs.organizationid
WHERE
    tokens.isvalid = true
GROUP BY
  orgs.accountid
LIMIT
    100