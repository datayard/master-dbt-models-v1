SELECT 
        account.id as accountId,
        account.account_number as accountNumber,
        account.created_by_id as createdById,
        account.created_date as createdDate,
        account.crm_id as crmId,
        account.customer_acquisition_date_c as customerAcquisitionDate,
        account.cx_department_c as cxDepartment,
        account.mrr as net_mrr,
        account.name as accountName,
        account.status as accountStatus,
        account.sales_rep_name as salesRepName,
        account.vidyard_id_c as vidyardId,
        account.sold_to_contact_id as soldToContactId,
        account.bill_to_contact_id as billToContactId,
        account.updated_by_id as updatedById,
        account.updated_date as updatedDate,
        account._fivetran_deleted as fivetranDeleted,
        account._fivetran_synced as fivetranSynced
FROM
        {{ source ('zuora', 'account')}} as account

WHERE
        TRUE