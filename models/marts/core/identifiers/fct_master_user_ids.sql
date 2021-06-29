SELECT	
	vyusers.email,
	vyusers.userid,
	sfdcleads.leadid as sfdcleadID,
	sfdccontact.contactid as sfdccontactID
FROM
	dbt_vidyard_master.stg_vidyard_users as vyusers
FULL OUTER JOIN dbt_vidyard_master.stg_salesforce_lead AS sfdcleads ON vyusers.userid = sfdcleads.vidyardUserId
FULL OUTER JOIN dbt_vidyard_master.stg_salesforce_contact AS sfdccontact ON vyusers.userid = sfdccontact.vidyarduserid
WHERE
	sfdcleads.isconverted = 'false'