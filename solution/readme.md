# beerwulf data engineer assessment by Marcus Klaas de Vries

## The star schema

- Why we dropped the nation and region tables.
- Why we only have a single fact table.


## Running the code

Provided python 3, sqlite3 and its associated python modules have been installed, loading the data into a sqlite3 database should be as simple as running the `load.py` file in the same directory as the extracted data files. It will create a `test.db` database file that contains the populated star scheme.

## Random order data

The primary concern with out-of-order data is that we may receive references to dimensions that have not been created yet. For example, a lineitem may be imported refering to a customer that has not yet been imported. The approach in my solution has been to import all dimensions first, and do the fact tables last. We exploit the star schema here, since it guarantees that the dimension tables themselves never refer to other tables.

When the data is coming from a stream, this won't work anymore. Instead, when we find a reference to an entity that does not exist yet, we could create a dummy entity (signaled by a flag) before inserting the lineitem. Assuming the data stream eventually does becoming consistent, we will later update the dummy entity with the real details when its id shows up in the stream and drop the dummy flag. Alternatively, we could hold off on inserting a lineitem when not all the entities it refers to exist yet and insert them when all its references have been created. This is a bit more risky since you'll lose data when entities don't show up.

## Deploying to production

The exact steps taken here would depend heavily on the environment the code would run in (own machines or cloud, in a container?), where the data would come from (simple files vs data streams, etc.) and where the data should be pushed to. Therefore I will take a more high level approach:

0. tests! Lots of tests. What happens when we find inconsistent data or unexpected data types (e.g. strings when we expected integers) or when we cannot connect to the database? We should write a few queries on test data and make sure the results are as expected.
1. set up a (continuous) deployment process so that new code is tracked, versioned and tested before it is released.
2. set up a way to share connection details and credentials. These are now hardcoded, but they should ideally be distributed in a more controlled manner.
3. set up tracking and logging. It is essential to know whether the load succeeded or not. Errors should be logged to a dashboard and alerts should be set up so that developers are made aware of issues.
4. schedule regular imports. This could be done through software like Airflow. Special care needs to be taken to ensure that all data is imported exactly once, even when there have been interruptions.

## Queries

### What are the bottom 3 nations in terms of revenue?
```sql
SELECT c_nationname, SUM(l_revenue) as nation_revenue FROM lineitems
	JOIN customers ON l_custkey = c_custkey
	GROUP BY c_nationname
	ORDER BY nation_revenue ASC
	LIMIT 3
```

### From the top 3 nations, what is the most common shipping mode?
```sql
WITH top_nations AS (
	SELECT c_nationname FROM lineitems
	JOIN customers ON l_custkey = c_custkey
	GROUP BY c_nationname
	ORDER BY SUM(l_revenue) DESC
	LIMIT 3
)

SELECT l_shipmode FROM lineitems
	JOIN customers ON l_custkey = c_custkey AND c_nationname IN top_nations
	GROUP BY l_shipmode
	ORDER BY COUNT(l_id) DESC
	LIMIT 1
```

### What are the top 5 selling months?
```sql
SELECT DATE(l_commitdate, 'start of month') as commitmonth FROM lineitems
	GROUP BY commitmonth
	ORDER BY SUM(l_revenue) DESC
	LIMIT 5
```

### Who are the top customers in terms of either revenue or quantity?
```sql
SELECT c_name, SUM(l_revenue) AS total_cust_value FROM customers
	JOIN lineitems ON l_custkey = c_custkey
	GROUP BY l_custkey
	ORDER BY total_cust_value DESC
```
Change `SUM(l_revenue) AS total_cust_value` to `SUM(l_quantity) AS total_cust_value` to weigh customers by quantity instead.

### Comparing the sales revenue on a financial year-to-year basis (July 1 to June 30)
```sql
SELECT strftime('%Y', DATE(l_commitdate, 'start of month', '+6 months')) AS financial_year, SUM(l_revenue) AS total_evenue FROM lineitems
	GROUP BY financial_year
	ORDER BY financial_year ASC
```
