/*{{ config(materialized='table', tags=['publish']) }}*/

SELECT DISTINCT
	user_master_table.vy_user_ID,
	vy_users.email,
	vy_users.createdDate as vy_user_created_at,
	vy_orgs.organizationID as vy_personalaccount_ID,
	CASE 
		WHEN	
			--vy_invoices.active = 't' 
			--vy_orgs.paying = 't'
			zuora_sub.status like 'Active'
			AND zuora_sub.mrr = '19.00'
			AND vy_orgs.ownerId = vy_users.userId
			AND vy_orgs.parentID IS null
			THEN 'Vidyard Pro'
		WHEN	
			--vy_invoices.active = 'f'
			vy_orgs.paying = 'f'
			AND vy_orgs.ownerId = vy_users.userId
			AND vy_orgs.parentID IS null
 			THEN 'Vidyard Free' --this is a downgraded account
 		WHEN	
			vy_orgs.organizationID NOT IN (SELECT accountId FROM {{ref('stg_vidyard_invoiced_customers')}})
			AND vy_orgs.ownerId = vy_users.userId
			AND vy_orgs.parentID IS null
 			THEN 'Vidyard Free' --this is a free account
		ELSE null
	END as type_of_account,
	CASE
		WHEN 
			type_of_account LIKE 'Vidyard Free'
			AND org_metrics.viewsCount >= '1'
			THEN 't'
		WHEN 
			type_of_account LIKE 'Vidyard Pro'
			AND org_metrics.viewsCount >= '1'--Using the same activated criteria for Free & Pro 
			THEN 't'
		WHEN 
			type_of_account LIKE 'Vidyard Free'
			AND (org_metrics.viewsCount <= '0'--Using the same activated criteria for Free & Pro 
			OR org_metrics.viewsCount IS null)
			THEN 'f'
		WHEN 
			type_of_account LIKE 'Vidyard Pro'
			AND (org_metrics.viewsCount <= '0'--Using the same activated criteria for Free & Pro 
			OR org_metrics.viewsCount IS null)
			THEN 'f'
		ELSE null
	END as activated,
	vy_orgs.createdDate as vy_personalaccount_created_date,
	vy_orgs.createdByClientID as vy_personalaccount_createdon,
	zuora_sub.createddate as vy_personalaccount_upgradedon
	--vy_invoices.created_at as vy_personalaccount_upgradedon --adding to push the date of the Pro-upgrade
FROM
	{{ ref('user_id_master') }} user_master_table
JOIN
	{{ref('stg_vidyard_users')}} as vy_users
ON
	user_master_table.vy_user_ID = vy_users.userId
JOIN
	{{ref('stg_vidyard_organizations')}} as vy_orgs
ON
	vy_orgs.ownerId = vy_users.userId
LEFT JOIN
	{{ref('stg_salesforce_account')}} as sfdc_account
ON
	vy_orgs.organizationID = sfdc_account.vidyardAccountId
LEFT JOIN
	{{ref('tier2_zuora')}} as zuora_sub
ON
	sfdc_account.accountId = zuora_sub.crmid	
LEFT JOIN
	{{ref('stg_vidyard_invoiced_customers')}} as vy_invoices
ON
	vy_invoices.accountId = vy_orgs.organizationID
LEFT JOIN
	{{ref('stg_vidyard_org_metrics')}} as org_metrics
ON
  	org_metrics.organizationID = vy_orgs.organizationID