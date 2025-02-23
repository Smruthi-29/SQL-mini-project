create database  mproj;
use mproj;
select * from cust_dimen;
select * from market_fact;
select * from orders_dimen;
select * from prod_dimen;
select * from shipping_dimen;


## 1) Joining  all the tables and creating a new table called combined_table

CREATE TABLE combined_table AS (
SELECT m.Ord_id, m.Prod_id, m.Ship_id, m.Cust_id, m.Sales, m.Discount, m.Order_Quantity, m.Profit, m.Shipping_Cost, m.Product_Base_Margin,
    c.Customer_Name, c.Province, c.Region, c.Customer_Segment,
    o.Order_ID, o.Order_Date, o.Order_Priority,
    p.Product_Category, p.Product_Sub_Category,
    s.Ship_Mode, s.Ship_Date 
    FROM market_fact mf
        JOIN
    cust_dimen c ON m.Cust_id = c.Cust_id
        JOIN
    orders_dimen o ON m.Ord_id = o.Ord_id
        JOIN
    prod_dimen p ON mf.Prod_id = p.Prod_id
        JOIN
    shipping_dimen s ON m.Ship_id = s.Ship_id);

select * from combined_table;

## 2. Find the top 3 customers who have the maximum number of orders

select  customer_Name, cust_id, order_id, count(customer_Name)as max_no_of_orders
from combined_table
group by cust_id
order by count(customer_Name) desc
limit 3;


## 3. Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.

alter table combined_table
add  DaysTakenForDelivery int as (DATEDIFF(STR_TO_DATE(Ship_Date , "%d-%m-%Y"),STR_TO_DATE(Order_Date , "%d-%m-%Y")));


## 4. Find the customer whose order took the maximum time to get delivered.

SELECT 
    Customer_Name,
    Customer_Segment,
    Order_Date,
    Ship_Date,
    DaysTakenForDelivery
FROM
    combined_table
WHERE
    DaysTakenForDelivery = (SELECT 
            MAX(DaysTakenForDelivery)
        FROM
            combined_table);


## 5. Retrieve total sales made by each product from the data (use Windows function).

select prod_id ,Product_Category,Product_Sub_Category,
round(sum(sales) over(partition by prod_id),2) as total_sales
from combined_table
order by total_sales desc;

## 6. Retrieve total profit made from each product from the data (use windows function).

select prod_id ,Product_Category, Product_Sub_Category,
round(sum(profit) over(partition by prod_id),2) as total_profit
from combined_table
order by total_profit desc;


## 7. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011.

SELECT  distinct Month(STR_TO_DATE(Order_Date,'%d-%m-%Y')) as months, count(cust_id) OVER
(PARTITION BY month(STR_TO_DATE(Order_Date,'%d-%m-%Y')) order by month(STR_TO_DATE(Order_Date,'%d-%m-%Y'))) AS
Total_Unique_Customers
FROM combined_table
WHERE year(STR_TO_DATE(Order_Date,'%d-%m-%Y'))=2011 AND cust_id
IN (SELECT DISTINCT cust_id
FROM combined_table
WHERE month(STR_TO_DATE(Order_Date,'%d-%m-%Y'))= 01
AND year(STR_TO_DATE(Order_Date,'%d-%m-%Y'))=2011);


## 8. Retrieve month-by-month customer retention rate since the start of the business.(using views).

#STEP 1: Create a view where each user’s visits are logged by month, allowing for the possibility that these will have occurred over multiple # years since whenever business started operations

Create view Visit_log AS 
	SELECT cust_id, TIMESTAMPDIFF(month,'2009-01-01', order_date) AS visit_month
	FROM combined_table
	GROUP BY 1,
			 2
	ORDER BY 1,
			 2;
select * from Visit_log;

# STEP 2: Identify the time lapse between each visit. So, for each person and for each month, we see when the next visit is.

Create view Time_Lapse  AS 
	SELECT distinct cust_id, 
					visit_month, 
					lead(visit_month, 1) over(
					partition BY cust_id 
					ORDER BY cust_id, visit_month) led
	FROM Visit_log;
select * from Time_Lapse ;


# STEP 3: Calculate the time gaps between visits:

Create view time_lapse_calculated as 
	SELECT cust_id,
           visit_month,
           led,
           led - visit_month AS time_diff 
	from Time_Lapse;
select * from  time_lapse_calculated;
    
    
# STEP 4:  categorise the customer with time gap 1 as retained, >1 as irregular and NULL as churned

Create view customer_category as	
    SELECT cust_id,
       visit_month,
       CASE
             WHEN time_diff=1 THEN "retained"
             WHEN time_diff>1 THEN "irregular"
             WHEN time_diff IS NULL THEN "churned"
       END as cust_category
FROM time_lapse_calculated;    
select * from customer_category;


# STEP 5: calculate the retention month wise


SELECT visit_month,(COUNT(if (cust_category="retained",1,NULL))/COUNT(cust_id)) AS retention
FROM customer_category  
GROUP BY 1 order by visit_month asc;

