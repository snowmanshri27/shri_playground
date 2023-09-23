create table if not exists orders ( 
    order_id integer,
    user_id integer, 
    eval_set varchar(10), 
    order_number integer,
    order_dow integer,
    order_hour_of_day integer, 
    days_since_prior_order integer
);