SELECT 
		sfdc_lead.id as leadId
		, sfdc_lead.isdeleted as isDeleted
		, sfdc_lead.account_id__c as accountId
		, LEFT(sfdc_lead.account_id__c, 15) as accountId_trimmed
		, sfdc_lead.vidyard_user__c as vidyardUserId
		, sfdc_lead.isconverted as isConverted
		, sfdc_lead.convertedcontactid as convertedContactId
		, sfdc_lead.createddate as createdDate
		, sfdc_lead.converteddate as convertedDate
		, sfdc_lead.lead_owner_id__c as leadOwnerId
		, sfdc_lead.lastname as lastName
		, sfdc_lead.firstname as firstName
		, sfdc_lead.name as name
		, sfdc_lead.email as email
		, sfdc_lead.title as title
		, sfdc_lead.role__c as role
		, sfdc_lead.company as company
		, sfdc_lead.industry as industry
		, sfdc_lead.leadsource as leadSource
		, sfdc_lead.lead_type__c as leadType
		, sfdc_lead.status as status
		, sfdc_lead.status_reason__c as statusReason
		, sfdc_lead.reason_unqualified__c as reasonUnqualified
		, sfdc_lead.baller_score__c as ballerScore

FROM 
		{{ source ('salesforce_production','lead') }} as sfdc_lead

WHERE
		TRUE