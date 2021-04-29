SELECT 
        product_rate_plan.id as productRatePlanId,
        product_rate_plan.active_currencies as activeCurrencies,
        product_rate_plan.created_by_id as CreatedById,
        product_rate_plan.created_date as createdDate,
        product_rate_plan.description as description,
        product_rate_plan.effective_end_date as effectiveEndDate,
        product_rate_plan.effective_start_date as effectiveStartDate,
        product_rate_plan.name as name,
        product_rate_plan.product_id as productId,
        product_rate_plan.updated_by_id as UpdatedByID,
        product_rate_plan.updated_date as updatedDate,
        product_rate_plan.visible_c as visible,
        product_rate_plan.billing_period_c as billingPeriod,
        product_rate_plan.pvstatus_c as pvStatus

FROM
        {{ source ('zuora', 'product_rate_plan')}} as product_rate_plan

WHERE
        TRUE