SELECT
        rpct.id as rateplanchargetierid
        , rpct.discount_percentage as discountpercentage
FROM
        {{ source ('zuora', 'rate_plan_charge_tier')}} as rpct

WHERE
        TRUE