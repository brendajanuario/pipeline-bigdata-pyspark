-- Databricks notebook source
--Countries with the most logged in users
SELECT COUNT(country) AS number_of_users, country 
FROM analytical.logs
WHERE action = 'login' 
GROUP BY country 
ORDER BY number_of_users
DESC LIMIT 10 


--Percentage of most viewed products
WITH top_login_country AS (
SELECT COUNT(country) c, country 
FROM analytical.logs 
WHERE action='login' 
GROUP BY country 
ORDER BY c DESC  LIMIT 1
)

SELECT COUNT(l.product_id) c, l.product_id, name AS product_name FROM analytical.logs l
INNER JOIN transactional.products p ON
p.product_id = l.product_id
WHERE action='product' and country = (SELECT country FROM top_login_country)
GROUP BY l.product_id, product_name 
ORDER BY c 
DESC LIMIT 5


--Main Devices Used
SELECT COUNT(user_agent) device_number, user_agent 
FROM analytical.logs 
GROUP BY user_agent 
ORDER BY device_number 
DESC LIMIT 5


--Total sales last year (monthly)
SELECT MONTH(order_dt) MONTH, COUNT(*) total_orders, AVG(order_total_price) AS total_price 
FROM transactional.orders 
WHERE order_dt > to_date(dateadd(YEAR, -1, current_date()))
GROUP BY month ORDER BY month

