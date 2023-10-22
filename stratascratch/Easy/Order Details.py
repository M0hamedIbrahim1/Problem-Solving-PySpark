# link : https://platform.stratascratch.com/coding/9913-order-details?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code

merge = customers.join(orders, customers.id == orders.cust_id , "inner")
customer = ["Jill", "Eva"]
result = merge.filter(F.col("first_name").isin(customer)).select(
    "first_name", "order_date", "order_details", "total_order_cost"
)
# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
