SELECT
        rate_plan.id as ratePlanID,
        rate_plan.amendment_id as amendmentID,
        rate_plan.amendment_subscription_rate_plan_id as amendmentSubscriptionRatePlanId,
        rate_plan.amendment_type as amendmentType,
        rate_plan.created_by_id as createdById,
        rate_plan.created_date as createdDate,
        rate_plan.name as name,
        rate_plan.product_rate_plan_id as productRatePlanId,
        rate_plan.subscription_id as subscriptionId,
        rate_plan.updated_by_id as updatedbyId,
        rate_plan.updated_date as updatedDate,
        rate_plan.sold_to_contact_id as soldToContactId,
        rate_plan.account_id as accountId,
        --rate_plan.product_id as productId,
        rate_plan.bill_to_contact_id as billtoContactId,
        rate_plan.subscription_version_amendment_id as subscriptionVersionAmendmentId,
        rate_plan.default_payment_method_id as defaultPaymentMethodId,
        rate_plan.triggersync_c as triggerSync,
        rate_plan.parent_account_id as parentAccountId

FROM
        {{ source ('zuora', 'rate_plan')}} as rate_plan

WHERE
        TRUE