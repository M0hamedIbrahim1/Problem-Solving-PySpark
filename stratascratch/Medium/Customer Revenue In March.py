# link   : https://platform.stratascratch.com/coding/9782-customer-revenue-in-march?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
orders = orders.filter((year(col('order_date')) == 2019) & (month(col('order_date')) == 3))
orders = orders.groupby('cust_id').agg(sum('total_order_cost').alias('total')).orderBy(desc('total'))

# To validate your solution, convert your final pySpark df to a pandas df
orders.toPandas()

