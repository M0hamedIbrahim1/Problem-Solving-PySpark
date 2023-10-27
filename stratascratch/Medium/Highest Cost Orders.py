# link   : https://platform.stratascratch.com/coding/9915-highest-cost-orders?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Start writing code
Trans_1 = orders.groupby('cust_id','order_date').agg(sum('total_order_cost').alias('max_cost'))
Trans_2 = Trans_1.withColumn('rnk' , dense_rank().over(Window.orderBy(desc('max_cost')) ))
Trans_3 = Trans_2.filter(col('rnk') == 1)

# let's join
merge = customers.join(Trans_3 , customers.id == Trans_3.cust_id , 'inner')
result = merge.select('first_name' , 'order_date' , 'max_cost')

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
