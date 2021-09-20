SELECT
    usrdetails.domain,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'free' then 1 end) as free,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'pro' then 1 end) as pro,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise' then 1 end) as enterprise,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise self serve' then 1 end) as enterprise_self_serve
FROM
    {{ ref('tier2_vidyard_user_details') }} usrdetails
    --dbt_vidyard_master.tier2_vidyard_user_details as usrdetails
WHERE
    usrdetails.domaintype like 'business'
GROUP BY
    usrdetails.domain
ORDER BY
    usrdetails.domain ASC