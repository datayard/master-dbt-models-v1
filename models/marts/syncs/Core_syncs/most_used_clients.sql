/*{{ config(materialized='table', tags=['publish']) }}*/

WITH 
	most_used_clients as(
		SELECT 
		  v.organizationID, 
		  v.userID,
		  vy_personalaccounts.email, 
		  vy_personalaccounts.vy_personalaccount_id,
		  v.createdByClientID as "most_frequent_client",
		  COUNT (v.createdByClientID) as "videos_from_client",
		  row_number() over(
		    partition by v.organizationID ORDER BY "videos_from_client" DESC)
		FROM 
		  {{ref('stg_vidyard_videos')}} as v
		LEFT JOIN 
		  {{ref('vy_personalaccount_object_data')}} as vy_personalaccounts
		ON 
		  v.organizationID = vy_personalaccounts.vy_personalaccount_id
		GROUP BY 
		  v.organizationID, 
		  v.userID,
		  vy_personalaccounts.email,
		  vy_personalaccounts.vy_personalaccount_id,
		  v.createdByClientID
)

SELECT 
	muc.organizationID,
	muc.userID,
	muc.most_frequent_client,
	muc.videos_from_client,
	CASE 
		WHEN muc.most_frequent_client LIKE 'secure.vidyard.com'
    		then 'Vidyard Platform' 
   		WHEN muc.most_frequent_client LIKE 'app.salesloft.com'
    		then 'Salesloft' 
    	WHEN muc.most_frequent_client LIKE 'auth.viewedit.com'
    		then 'GoVideo Extension' 
    	WHEN muc.most_frequent_client LIKE 'mixmax.com'
    		then 'Mixmax' 
    	WHEN muc.most_frequent_client LIKE 'app.hubspot.com'
    		then 'Hubspot App' 
    	WHEN muc.most_frequent_client LIKE 'sales.hubspot.com'
    		then 'Hubspot sales' 
    	WHEN muc.most_frequent_client LIKE 'support.hubspot.com'
    		then 'Hubspot support' 
    	WHEN muc.most_frequent_client LIKE 'outlook.vidyard.com'
    		then 'Outlook' 
    	WHEN muc.most_frequent_client LIKE 'govideo-mobile.vidyard.com'
    		then 'GoVideo Mobile'
    	WHEN muc.most_frequent_client LIKE 'quip.com'
    		then 'Quip' 
    	WHEN muc.most_frequent_client LIKE 'drift.com'
    		then 'Drift'
    	WHEN muc.most_frequent_client LIKE 'allbound.com'
    		then 'Allbound'
    	WHEN muc.most_frequent_client LIKE 'prod.outreach.io'
    		then 'Outreach'
  ELSE muc.most_frequent_client
END AS client_name
FROM 
	most_used_clients as muc
WHERE 
	muc.row_number = '1'