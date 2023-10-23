# link   : https://platform.stratascratch.com/coding/2119-most-lucrative-products?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
Transformation_1 = online_orders.filter((month(online_orders['date']) >= 1) & (month(online_orders['date']) <= 6))
Transformation_2 = Transformation_1.withColumn('total' ,online_orders['units_sold'] * online_orders['cost_in_dollars'] )
Transformation_3 = Transformation_2.groupby('product_id').agg(sum('total').alias('total')).sort('total' , ascending = False).limit(5)

# To validate your solution, convert your final pySpark df to a pandas df
Transformation_3.toPandas()

