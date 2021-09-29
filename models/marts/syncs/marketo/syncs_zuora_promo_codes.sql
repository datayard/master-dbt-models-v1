select 

mkto_lead.leadId,
promo_codes.promo,
promo_codes.promostartdate,
promo_codes.promoenddate,
promo_codes.organizationid


from dbt_vidyard_master.tier2_promo_codes as promo_codes

JOIN
  {{ref('user_id_master')}} as master_user
ON
  promo_codes.userid = master_user.vy_user_id
JOIN
  {{ref('stg_marketo_lead')}} as mkto_lead
ON
  mkto_lead.leadId = master_user.mkto_lead_id
WHERE 
  master_user.mkto_lead_id IS NOT null