/*
=======================================
Product Report
=======================================

Purpose:
	- This report consolidates key product metrics and behaviours.

Highlihts:
	- 1. Gathers essential fields such as product name, category, subcategory and cost.
	- 2. Segments products by revenue to identify High Performers, Mid-Range or Low Performers.
	- 3. Aggregates product-level metrics:
		- total orders
		- total sales amount
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	- 4. Calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue

------------------------------------------------------
*/

CREATE VIEW gold.product_report AS

WITH base_query AS (
-- 1. Base query: Retrieves core columns from fact_sales and dim_products

	SELECT
		f.order_number,
		f.order_date,
		f.customer_key,
		f.sales_amount,
		f.quantity,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON  f.product_key = p.product_key
	WHERE order_date IS NOT NULL -- only consider valid sales dates
)


, product_aggregations AS (
-- 2. Product aggregations: Summarizes key metrics at the product level
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
	MAX(order_date) as last_sale_date,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(sales_amount) AS total_sales_amount,
	SUM(quantity) AS total_quantity,
	ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS average_selling_price
FROM base_query
GROUP BY 
	product_key,
	product_name,
	category,
	subcategory,
	cost
)

-- 3. Final query: Combines  all product results into  one output

SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency,
	CASE WHEN total_sales_amount > 50000 THEN 'High-Performer'
		 WHEN total_sales_amount >= 10000 THEN 'Mid-Range'
		 ELSE 'Low-Performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_sales_amount,
	total_quantity,
	total_customers,
	average_selling_price,

	-- Average Order Revenue (AOR)
	CASE WHEN total_sales_amount = 0 THEN 0
		 ELSE total_sales_amount / total_orders 
	END AS avg_order_revenue,

	-- Average Monthly Revenue
	CASE WHEN lifespan = 0 then total_sales_amount
		 ELSE total_sales_amount/lifespan
	END AS avg_monthly_revenue
FROM product_aggregations