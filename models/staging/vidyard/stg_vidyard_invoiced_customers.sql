SELECT 

	vy_invoiced_customers.account_id as accountId
	,vy_invoiced_customers.invoiced_customer_id as invoicedCustomerId
    ,vy_invoiced_customers.active as active
    ,vy_invoiced_customers.created_at as createdAt
    ,vy_invoiced_customers.updated_at as updatedAt

 FROM 
 
	{{ source('public', 'vidyard_invoiced_customers') }} as vy_invoiced_customers