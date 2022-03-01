/*{{ config(materialized='table', tags=['publish']) }}*/

WITH
  sfdc_notconverted_leads as (
    SELECT
      sfdc_leads_og.leadId,
      sfdc_leads_og.email
    FROM  
      {{ ref('stg_salesforce_lead') }} as sfdc_leads_og
    WHERE
      sfdc_leads_og.isConverted = 'False'
  )

SELECT
  vy_users.email,
  split_part(vy_users.email, '@', 2) as email_domain,
  vy_users.userId as vy_user_ID,
  sfdc_contacts.contactId as sfdc_contact_ID,
  sfdc_leads.leadId as sfdc_lead_ID,
  mkto_leads.leadId as mkto_lead_ID, 
  heap_users.userID as heap_id
FROM
  {{ref('stg_vidyard_users')}} as vy_users
LEFT JOIN
  {{ref('stg_salesforce_contact')}} as sfdc_contacts
ON
  vy_users.email = sfdc_contacts.email
LEFT JOIN
  sfdc_notconverted_leads as sfdc_leads
ON
  vy_users.email = sfdc_leads.email
LEFT JOIN
  {{ref('stg_marketo_lead')}} as  mkto_leads
ON
  vy_users.email = mkto_leads.emailAddress
LEFT JOIN
  {{ref('stg_govideo_production_users')}} as  heap_users
ON
  vy_users.userId = cast(heap_users.identifier as varchar(10))
WHERE
  true