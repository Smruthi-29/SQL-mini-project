# SQL-mini-project


Composite data of a business organisation, confined to ‘sales and delivery’
domain is given for the period of last decade. From the given data retrieve 
solutions for the given scenario.
1. Join all the tables and create a new table called combined_table.
(market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)
2. Find the top 3 customers who have the maximum number of orders
3. Create a new column DaysTakenForDelivery that contains the date difference 
of Order_Date and Ship_Date.
4. Find the customer whose order took the maximum time to get delivered.
5. Retrieve total sales made by each product from the data (use Windows 
function)
6. Retrieve total profit made from each product from the data (use windows 
function)
7. Count the total number of unique customers in January and how many of them 
came back every month over the entire year in 2011
8. Retrieve month-by-month customer retention rate since the start of the 
business.(using views)
Tips:
#1: Create a view where each user’s visits are logged by month, allowing for 
the possibility that these will have occurred over multiple # years since 
whenever business started operations
# 2: Identify the time lapse between each visit. So, for each person and for each 
month, we see when the next visit is.
# 3: Calculate the time gaps between visits
# 4: categorise the customer with time gap 1 as retained, >1 as irregular and 
NULL as churned
# 5: calculate the retention month wise
