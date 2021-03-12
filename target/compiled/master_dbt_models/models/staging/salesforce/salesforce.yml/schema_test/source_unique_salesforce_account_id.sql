
    
    



select count(*) as validation_errors
from (

    select
        id

    from "dev"."salesforce_production"."account"
    where id is not null
    group by id
    having count(*) > 1

) validation_errors


