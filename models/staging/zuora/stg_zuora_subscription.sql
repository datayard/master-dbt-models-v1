SELECT 
        subscription.id as subscriptionId,
        subscription.account_id as accountId,
        subscription.cancelled_date as cancelledDate,
        subscription.contract_effective_date as contractStartDate,
        subscription.contract_effective_date as contractEffectiveDate,
        subscription.service_activation_date as serviceActivationDate,
        subscription.created_by_id as createdById,
        subscription.created_date as createdDate,
        subscription.current_term as currentTerm,
        subscription.current_term_period_type as currentTermPeriodType,
        subscription.initial_term as initialTerm,
        subscription.initial_term_period_type as initialTermPeriodType,
        subscription.name as name,
        subscription.original_created_date as originalCreatedDate,
        subscription.original_id as originalSubscriptionId,
        subscription.previous_subscription_id as previousSubscriptionId,
        subscription.renewal_term as renewalTerm,
        subscription.renewal_term_period_type as renewalTermPeriodType,
        subscription.status as status,
        subscription.subscription_end_date as subscriptionEndDate,
        subscription.subscription_start_date as subscriptionStartDate,
        subscription.term_end_date as termEndDate,
        subscription.term_start_date as termStartDate,
        subscription.term_type as termType,
        subscription.updated_by_id as updatedById,
        subscription.updated_date as updatedDate,
        subscription.vidyard_cancel_date_c as vidyardCancelDate,
        subscription._fivetran_deleted as fivetranDeleted,
        subscription.sold_to_contact_id as soldToContactId,
        subscription.subscription_version_amendment_id as subscriptionVersionAmendmentId,
        subscription.default_payment_method_id as defaultPaymentMethodId,
        subscription.bill_to_contact_id as billToContactId,
        subscription.version as subscriptionversion,
        subscription.promocode_c as promocode

FROM
    {{ source ('zuora', 'subscription')}} as subscription

WHERE

    TRUE
