# link   : https://platform.stratascratch.com/coding/9891-customer-details?code_type=6
# author : Mohamed Ibrahim

import pyspark.sql.functions as F

merged_table = customers.join(orders, customers.id == orders.cust_id, "left")
result = merged_table.select("first_name", "last_name", "city", "order_details").sort("first_name", "order_details")
result.toPandas()


