SELECT 
    sfdc_account.id as accountId,
    sfdc_account.name as accountName,
    sfdc_account.isdeleted as isDeleted,
    sfdc_account.type as accountType,
    sfdc_account.parentid as parentAccountId,
    sfdc_account.phone as accountPhone,
    sfdc_account.industry as accountIndustry,
    sfdc_account.employee_segment__c as employeeSegment,
    sfdc_account.numberofemployees as numberOfEmployees,
    sfdc_account.annualrevenue as annualRevenue,
    sfdc_account.ownerid as ownerId,
    sfdc_account.customer_tier__c as customerTier,
    sfdc_account.email_domain__c as emailDomain,
    sfdc_account.website as website,
    sfdc_account.account_id__c as vidyardAccountId,
    sfdc_account.original_contract_date__c as originalContractDate,
    sfdc_account.csm_owner__c as csmOwnerId,
    sfdc_account.ispersonaccount as isPersonAccount,
    sfdc_account.self_serve_customer__c as isSelfServe,
    sfdc_account.billingstreet as billingStreet,
    sfdc_account.billingcity as billingCity,
    sfdc_account.billingstate as billingState,
    sfdc_account.billingpostalcode as billingPostalCode,
    sfdc_account.billingcountry as billingCountry,
    sfdc_account.ispartner as isPartner,
    sfdc_account.date_of_churn__c as dateOfChurn,
    sfdc_account.churn_reason__c as churnReason,
    sfdc_account.churn_reason_details__c as churnReasonDetails,
    sfdc_account.partner_type2__c as partnerType,
    sfdc_account.zvidyard_Account_Products__c as vidyardAccountProducts,
    sfdc_account.created_datetime__c as createdDate,
    sfdc_account.new_territory__c as accountTerritory,
    sfdc_account.region__c as accountRegion,
    sfdc_account.non_contract__c as nonContract,
    sfdc_account.vidyard_user_id__pc as vidyardUserId,
    sfdc_account.crm__c as crmPlatform,
    sfdc_account.marketing_automation_platform__c as marketingAutomationPlatform,
    sfdc_account.abm_tier__c as abmTier,
    sfdc_account.online_video_platform__c as onlineVideoPlatform,
    sfdc_account.non_contract__c as nonContract,
    sfdc_account.primary_use_case__c as primaryUseCase,
    sfdc_account.qa_status__c as qaStatus 
 FROM 
    {{ source('salesforce_production', 'account') }} as sfdc_account
WHERE
    sfdc_account.type LIKE 'Customer'
    AND sfdc_account.ispersonaccount = false