use sakila;



select first_name, last_name
from actor;



select upper(concat(first_name, ' ', last_name))   'Actor Name'
from actor;



select actor_id, first_name, last_name
from actor
where lower(first_name) = lower("Joe");



select *
from actor
where upper(last_name) like '%GEN%';





select country_id, country
from country
where country in ('Afghanistan', 'Bangladesh', 'China');



select * from actor;

ALTER TABLE actor
    ADD COLUMN description BLOB;

select * from actor;



select * from actor;

alter table actor
drop column description;

select * from actor;



select last_name, count(*) actor_count
from actor
group by last_name
order by actor_count desc, last_name;



select last_name, count(*) actor_count
from actor
group by last_name
having actor_count >1
order by actor_count desc, last_name;



select * from actor where first_name = 'GROUCHO' and last_name = 'WILLIAMS';

update actor set first_name = 'HARPO', last_name = 'WILLIAMS' where first_name = 'GROUCHO' and last_name = 'WILLIAMS';

select * from actor where last_name = 'WILLIAMS';



update actor set first_name = 'GROUCHO', last_name = 'WILLIAMS' where first_name = 'HARPO' and last_name = 'WILLIAMS';

select * from actor where last_name = 'WILLIAMS';



SHOW CREATE TABLE address;



select stf.first_name, stf.last_name, adr.address, adr.district, adr.postal_code, adr.city_id
from staff stf
         left join address adr
                   on stf.address_id = adr.address_id;



select stf.first_name, stf.last_name, sum(pay.amount)
from staff stf
         left join payment pay
                   on stf.staff_id = pay.staff_id
WHERE month(pay.payment_date) = 8
  and year(pay.payment_date)  = 2005
group by stf.first_name, stf.last_name;



select flm.title, count(*) number_of_actors
from film flm
         inner join film_actor fim_act
                    on flm.film_id = fim_act.film_id
group by flm.title
order by number_of_actors desc;



select flm.title, count(*) number_in_inventory
from film flm
         inner join inventory inv
                    on flm.film_id = inv.film_id
where lower(flm.title) = lower('Hunchback Impossible')
group by flm.title;



select cust.first_name, cust.last_name, sum(pay.amount) 'Total Amount Paid'
from payment pay
         join customer cust
              on pay.customer_id = cust.customer_id
group by cust.first_name, cust.last_name
order by cust.last_name;



select title
from film
where (title like 'K%' or title like 'Q%')
  and language_id in (
    select language_id
    from language
    where name = 'English'
)
order by title;



select first_name, last_name
from actor
where actor_id in (
    select actor_id
    from film_actor
    where film_id in (
        select film_id from film where lower(title) = lower('Alone Trip')
    )
);




select first_name, last_name, email
from customer
where address_id in (
    select address_id
    from address
    where city_id in (
        select city_id
        from city
        where country_id in (
            select country_id
            from country
            where country = 'Canada'
        )
    )
);



select cus.first_name, cus.last_name, cus.email
from customer cus
         join address adr
              on cus.address_id = adr.address_id
         join city cit
              on adr.city_id = cit.city_id
         join country cou
              on cit.country_id = cou.country_id
where cou.country = 'Canada';



select film_id, title, release_year
from film
where film_id in (
    select film_id
    from film_category
    where category_id in (
        select category_id
        from category
        where name = 'Family'
    )
);



select A.film_id, A.title, B.*
from film A
         join (
    select inv.film_id, count(ren.rental_id) times_rented
    from rental ren
             join inventory inv
                  on ren.inventory_id = inv.inventory_id
    group by inv.film_id
) B
              on A.film_id = B.film_id
order by B.times_rented desc;



select A.store_id, B.sales
from store A
         join (
    select cus.store_id, sum(pay.amount) sales
    from customer cus
             join payment pay
                  on pay.customer_id = cus.customer_id
    group by cus.store_id
) B
              on A.store_id = B.store_id
order by a.store_id;



select sto.store_id, cit.city, cou.country
from store sto
         left join address adr
                   on sto.address_id = adr.address_id
         join city cit
              on adr.city_id = cit.city_id
         join country cou
              on cit.country_id = cou.country_id;

select A.*, B.sales
from (
         select sto.store_id, cit.city, cou.country
         from store sto
                  left join address adr
                            on sto.address_id = adr.address_id
                  join city cit
                       on adr.city_id = cit.city_id
                  join country cou
                       on cit.country_id = cou.country_id
     ) A
         join (
    select cus.store_id, sum(pay.amount) sales
    from customer cus
             join payment pay
                  on pay.customer_id = cus.customer_id
    group by cus.store_id
) B
              on A.store_id = B.store_id
order by a.store_id;



select cat.name category_name, sum( IFNULL(pay.amount, 0) ) revenue
from category cat
         left join film_category flm_cat
                   on cat.category_id = flm_cat.category_id
         left join film fil
                   on flm_cat.film_id = fil.film_id
         left join inventory inv
                   on fil.film_id = inv.film_id
         left join rental ren
                   on inv.inventory_id = ren.inventory_id
         left join payment pay
                   on ren.rental_id = pay.rental_id
group by cat.name
order by revenue desc
    limit 5;



create view top_five_genres as
select cat.name category_name, sum( IFNULL(pay.amount, 0) ) revenue
from category cat
         left join film_category flm_cat
                   on cat.category_id = flm_cat.category_id
         left join film fil
                   on flm_cat.film_id = fil.film_id
         left join inventory inv
                   on fil.film_id = inv.film_id
         left join rental ren
                   on inv.inventory_id = ren.inventory_id
         left join payment pay
                   on ren.rental_id = pay.rental_id
group by cat.name
order by revenue desc
    limit 5;



select * from top_five_genres;



drop view top_five_genres;