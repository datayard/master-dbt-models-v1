SELECT 
    sfdc_contact.id as contactId
    , sfdc_contact.isdeleted as isDeleted
    , sfdc_contact.accountid as accountId
    , LEFT(sfdc_contact.accountid, 15) as accountId_trimmed
    , sfdc_contact.vidyard_user_id__c as vidyardUserId
    , sfdc_contact.createddate as createdDate
    , sfdc_contact.account_owner_id__c as accountOwnerId
    , sfdc_contact.ownerid as ownerId
    , sfdc_contact.lastname as lastName
    , sfdc_contact.firstname as firstName
    , sfdc_contact.name as name
    , sfdc_contact.email as email
    , sfdc_contact.title as title
    , sfdc_contact.role__c as role
    , sfdc_contact.job_role__c as jobRole
    , sfdc_contact.department__c as department
    , sfdc_contact.account_lead_type__c as accountLeadType
    , sfdc_contact.leadsource as leadSource
    , sfdc_contact.baller_score__c as ballerScore
    , sfdc_contact.contact_status_vy__c as contactStatus
    , sfdc_contact.status_reason__c as statusReason
    , sfdc_contact.main_csm_contact__c as mainContact
    , sfdc_contact.marketing_automation_platform__c as marketingAutomationPlatform
    , sfdc_contact.crm__c as crm
    , sfdc_contact.mailingstreet as mailingStreet
    , sfdc_contact.mailingcity as mailingCity
    , sfdc_contact.mailingstate as mailingState
    , sfdc_contact.mailingpostalcode as mailingPostalCode
    , sfdc_contact.mailingcountry as mailingCountry 
 FROM 
    {{ source('salesforce_production', 'contact') }} as sfdc_contact