/*
=====================================
Customer Report
=====================================

Purpose
	- This report consolidates key customer metrics and behaviours

Highlights:
	1. Gather essential fields as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregate customer level metrics:
		- Total orders
		- Total sales
		- Total quantity purchased
		- Total products
		- Lifespan (in months)
	4. Calculate valuable KPIs:
		- rencecy (months since last order)
		- average order value
		- average monthly spends
----------------------------------------------------------------------------------
*/

CREATE VIEW gold.customer_report AS

WITH base_query AS(
-- 1. Base Query Retrieve Core columns from table
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ' , c.last_name) AS customer_name,
DATEDIFF(YEAR, c.birthdate, GETDATE()) AS age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
WHERE order_date IS NOT NULL)

, customer_aggregation AS ( 
-- 2. Customer Aggregations: Summarizes key metrics at the customer level
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(sales_amount) AS total_sales_amount,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT product_key) AS total_products,
	MAX(order_date) AS last_order_date,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
FROM base_query
GROUP BY 
	customer_key,
	customer_number,
	customer_name,
	age)
	

SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE WHEN age < 20 THEN 'Under 20'
		 WHEN age BETWEEN 20 AND 29 THEN '20-29' 
		 WHEN age BETWEEN 30 AND 39 THEN '30-39'
		 WHEN age BETWEEN 40 AND 49 THEN '40-49' 
		 ELSE '50 and above'
	END AS age_group,
	CASE WHEN lifespan >= 12 AND total_sales_amount > 5000 THEN 'VIP'
		 WHEN lifespan >= 12 AND total_sales_amount <= 5000 THEN 'Regular'
		 ELSE 'New'
	END AS customer_segment,
	last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency,
	total_orders,
	total_sales_amount,
	total_quantity,
	total_products,
	lifespan,
	-- Compute average order value (AVO)
	CASE WHEN total_orders = 0 THEN 0
		 ELSE total_sales_amount / total_orders
		END  as average_order_value,

	-- Compure average monthly spends
	CASE WHEN lifespan = 0 THEN total_sales_amount
		 ELSE total_sales_amount/lifespan
	END AS average_monthly_spend
FROM customer_aggregation;

