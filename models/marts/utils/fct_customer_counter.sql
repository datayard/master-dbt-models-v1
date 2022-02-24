WITH pro_acccounts as (
    SELECT
        'Self-serve' as accounttype,
        COUNT (distinct Pro_accounts.activeproaccounts) as accounts
    FROM
        {{ ref('fct_active_pro_subscriptions') }} as Pro_accounts
        --dbt_vidyard_master.fct_active_pro_subscriptions as Pro_accounts
    WHERE
          Pro_accounts.name NOT LIKE 'Free Forever'
    ),

    commercial_accounts as (
    SELECT
        'commercial_accounts' as accounttype,
        COUNT (distinct account.accountid) as accounts
    FROM
        {{ ref('stg_salesforce_account') }} as account
        --dbt_vidyard_master.stg_salesforce_account as account
    WHERE
        account.accounttype LIKE 'Customer'
        AND account.employeesegment LIKE 'Commercial'
        AND account.ispersonaccount = false
    ),

    emerging_accounts as (
    SELECT
        'emerging_accounts' as accounttype,
        COUNT (distinct account.accountid) as accounts
    FROM
        {{ ref('stg_salesforce_account') }} as account
        --dbt_vidyard_master.stg_salesforce_account as account
    WHERE
        account.accounttype LIKE 'Customer'
        AND account.employeesegment NOT LIKE 'Commercial'
        AND account.ispersonaccount = false
    )

SELECT
    *
FROM
    (
        SELECT
            accounttype,
            accounts
        FROM pro_acccounts
        UNION ALL
        SELECT
            accounttype,
            accounts
        FROM commercial_accounts
        UNION ALL
        SELECT
            accounttype,
            accounts
        FROM emerging_accounts
    )
GROUP BY
    accounttype,
    accounts