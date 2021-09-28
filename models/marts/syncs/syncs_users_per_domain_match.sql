WITH maus as (
    SELECT
        COUNT (distinct mau.userid) as maus,
        mau.domain
    FROM
        dbt_vidyard_master.tier2_mau as mau
    WHERE
        mau = 1
    GROUP BY
        mau.domain
),
    WEU as (
        SELECT
            COUNT (distinct weu.userid) as weus,
            weu.domain
        FROM
            dbt_vidyard_master.tier2_weu as weu
        WHERE
            true
        GROUP BY
            weu.domain
    )

SELECT
    usrdetails.domain,
    maus.maus,
    WEU.weus,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'free' then 1 end) as free,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'pro' then 1 end) as pro,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise' then 1 end) as enterprise,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise self serve' then 1 end) as enterprise_self_serve
FROM
    {{ ref('tier2_vidyard_user_details') }} usrdetails
    --dbt_vidyard_master.tier2_vidyard_user_details as usrdetails
LEFT JOIN
    maus
ON
    maus.domain = usrdetails.domain
LEFT JOIN
    WEU
ON
    WEU.domain = usrdetails.domain
WHERE
    usrdetails.domaintype like 'business'
    AND usrdetails.createddate  >= DATEADD(MONTH, -6, current_date)
GROUP BY
    usrdetails.domain,
    maus.maus,
    WEU.weus
ORDER BY
    maus.maus ASC





