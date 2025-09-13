USE DataWarehouse;

-- ====================================
-- Changes Overtime Analysis
-- ====================================
-- Yearly trends
SELECT
YEAR(order_date) AS order_year,
SUM(sales_amount) AS total_sales_amount,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY order_year;


-- Monthly trends
SELECT
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_sales_amount,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY order_year, order_month;

-- Alternative
SELECT
DATETRUNC(MONTH, order_date) AS order_date,
SUM(sales_amount) AS total_sales_amount,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY order_date;

-- Alternative (Will be issue in sorting)
SELECT
FORMAT(order_date, 'yyyy-MMM') AS order_date,
SUM(sales_amount) AS total_sales_amount,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY order_date;


-- ====================================
-- Cumulative Analysis
-- ====================================

-- Calculate total sales per month
-- and running total of sales over time and moving average
SELECT
order_date,
total_sales_amount,
SUM(total_sales_amount) OVER(PARTITION BY DATETRUNC(YEAR, order_date) ORDER BY order_date ASC) AS running_total_sales_amount,
AVG(avg_price) OVER(PARTITION BY DATETRUNC(YEAR, order_date) ORDER BY order_date ASC) AS moving_average_price
FROM
	(SELECT
	DATETRUNC(MONTH, order_date) AS order_date,
	SUM(sales_amount) AS total_sales_amount,
	AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(MONTH, order_date)) t;


-- ====================================
-- Performance Analysis
-- ====================================


-- Analyze yearly performance of products by comparing their sales
-- to both average sales performance of the product and previous year's sales
WITH yearly_product_sales AS (
	SELECT
	YEAR(f.order_date) AS order_year,
	p.product_name,
	SUM(f.sales_amount) AS current_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
	WHERE order_date IS NOT NULL
	GROUP BY YEAR(f.order_date), p.product_name
)


SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) as average_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) as diff_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0
	 THEN 'Above Avg'
	 WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0
	 THEN 'Below Avg'
	 ELSE 'Avg'
END AS avg_change,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) as diff_prev_year,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0
	 THEN 'Increase'
	 WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0
	 THEN 'Decrease'
	 ELSE 'No Change'
END AS prev_year_change
FROM yearly_product_sales
ORDER BY product_name, order_year;


-- ==================================
-- Part-To-Whole Analysis
-- ==================================

-- Which categories contribute the most to overall sales
WITH category_sales as (
	SELECT 
	p.category,
	SUM(f.sales_amount) total_sales_amount
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	GROUP BY p.category)
	
SELECT
category,
total_sales_amount,
SUM(total_sales_amount) OVER () AS overall_sales,
CONCAT(ROUND((CAST(total_sales_amount AS FLOAT)/SUM(total_sales_amount) OVER ()) * 100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales_amount DESC;


-- ================================
-- Data Segmentation
-- ================================

-- Segment products into cost ranges and
-- count how many products fall into each segment
WITH product_segments as (
	SELECT
	product_key,
	product_name,
	cost,
	CASE WHEN cost < 100 THEN 'Below 100'
		 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
		 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		 ELSE 'Above 1000'
	END AS cost_range
	FROM gold.dim_products)
	
SELECT 
cost_range,
COUNT(product_key) as total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;


-- Group Customers into 3 segments based on their spend behaviour:
--	- VIP: Customesr with atleast 12 months of history and spending more than 5000.
--  - Regular: Customers with atleast 12 months of history but spending 5000, or less.
--  - New: Customers with lifespan less than 12 months.
-- And find total number of customers by each group
WITH customer_spending AS (
	SELECT 
	customer_key,
	SUM(sales_amount) as total_spends,
	MIN(order_date) first_order_date,
	MAX(order_date) last_order_date,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM gold.fact_sales
	GROUP BY customer_key)

SELECT 
customer_segment, 
COUNT(customer_key) AS total_customers
FROM (SELECT 
	customer_key,
	CASE WHEN lifespan >= 12 AND total_spends > 5000 THEN 'VIP'
		 WHEN lifespan >= 12 AND total_spends <= 5000 THEN 'Regular'
		 ELSE 'New'
	END AS customer_segment
	FROM customer_spending) t
GROUP BY customer_segment
ORDER BY total_customers DESC;
