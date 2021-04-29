SELECT
        product.id as productId,
        product.allow_feature_changes as allowFeatureChanges,
        product.category as category,
        product.created_by_id as createdById,
        product.created_date as createdDate,
        product.description as description,
        product.effective_end_date as effectiveEndDate,
        product.effective_start_date as effectiveStartDate,
        product.name as name,
        product.optional_for_c as optionalFor,
        product.plan_type_c as planType,
        product.price_book_c as priceBook,
        product.sku as sku,
        product.sub_category_c as subCategory,
        product.updated_by_id as updatedById,
        product.updated_date as updatedDate

FROM

        {{ source ('zuora', 'product')}} as product

WHERE
        TRUE