{{ config(materialized='table', tags=['publish']) }}

SELECT 
	account_master_table.vy_id,
	account_master_table.sfdc_id
FROM
	{{ ref('account_id_master') }} account_master_table
WHERE
	account_master_table.sfdc_id IS NOT NULL
