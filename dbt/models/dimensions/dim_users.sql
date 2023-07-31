with
    source as (select * from {{ source("instacart_raw_data", "stg_users") }}),
    renamed as (
        SELECT a.user_id, 
            a.first_name, 
            a.last_name,
            a.gender,
            a.age, 
            LOWER(a.first_name) || '.' || LOWER(a.last_name) || '@' || a.email_domain AS email
        FROM (  
            SELECT row_number() over() AS user_id, 
                "Firstname" AS first_name,
                "Lastname" AS last_name,
                "Gender" AS gender,
                "Age" AS age,
                (array[
                    'gmail.com', 
                    'hotmail.com', 
                    'yahoo.com'
                ])[floor(random() * 3 + 1)] AS email_domain
            FROM "stg_users"
        ) a
    )
select *
from renamed
