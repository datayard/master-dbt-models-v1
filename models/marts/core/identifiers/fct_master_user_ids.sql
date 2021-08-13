SELECT	
	vyusers.email,
	vyusers.userid,
	sfdcleads.leadid as sfdcleadID,
	sfdccontact.contactid as sfdccontactID
FROM
	{{ ref('stg_vidyard_users') }} as vyusers
FULL OUTER JOIN {{ ref('stg_salesforce_lead') }} as sfdcleads ON vyusers.userid = sfdcleads.vidyardUserId
FULL OUTER JOIN {{ ref('stg_salesforce_contact') }} as sfdccontact ON vyusers.userid = sfdccontact.vidyarduserid
WHERE
	sfdcleads.isconverted = 'false'