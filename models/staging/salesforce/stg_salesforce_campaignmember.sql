SELECT 
		sfdc_campaignmember.id as campaignMemberId,
		sfdc_campaignmember.createddate as createdDate,
		sfdc_campaignmember.isdeleted as isDeleted,
		sfdc_campaignmember.campaignid as campaignId,
		sfdc_campaignmember.type as type,
		sfdc_campaignmember.leadid as leadId,
		sfdc_campaignmember.contactid as contactId,
		sfdc_campaignmember.status as status,
		sfdc_campaignmember.name as campaignMemberName,
		sfdc_campaignmember.email as email,
		sfdc_campaignmember.campaign_sourced_by__c as campaignSourcedBy,
		sfdc_campaignmember.mql__c as mql,
		sfdc_campaignmember.mql_date_and_time__c as mqlDateGMT,
		dateadd(hour,-4,sfdc_campaignmember.mql_date_and_time__c)as mqlDateEST,
		sfdc_campaignmember.sal__c as sal,
		sfdc_campaignmember.sal_date_and_time__c as salDateGMT,
		dateadd(hour,-4,sfdc_campaignmember.sal_date_and_time__c)as salDateEST,
		sfdc_campaignmember.sql__c as sql,
		sfdc_campaignmember.sql_date_and_time__c as sqlDateGMT,
		dateadd(hour,-4,sfdc_campaignmember.sql_date_and_time__c)as sqlDateEST,
		sfdc_campaignmember.sqo__c as sqo,
		sfdc_campaignmember.sqo_date__c as sqoDate,
		sfdc_campaignmember.sqo_lost__c as sqoLost,
		sfdc_campaignmember.sqo_lost_date__c as sqoLostDate,
		sfdc_campaignmember.won__c as won,
		sfdc_campaignmember.opportunity_closed_won_date__c as opportunityClosedWonDate,
		sfdc_campaignmember.opportunity__c as opportunityId,
		sfdc_campaignmember.status_reason__c as statusReason,
		sfdc_campaignmember.current_status_reason__c as currentStatusReason

 FROM 
 		{{ source ('salesforce_production' , 'campaignmember') }} as sfdc_campaignmember
 WHERE
 		TRUE
