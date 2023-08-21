SELECT 
    d.department, 
    case when b.gender = 1 then 'female'
        when b.gender = 2 then 'male'
    end as gender,
    count(distinct a.order_id) as orders, 
    count(distinct a.user_id) as unique_users
FROM fct_user_order_products a
JOIN dim_users b
    on a.user_id = b.user_id 
JOIN dim_products c
    on a.product_id = c.product_id
JOIN dim_departments d
    on c.department_id = d.department_id
GROUP BY 1, 2
ORDER by orders desc
