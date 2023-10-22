# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
worker = worker.filter(month(worker['joining_date']) >= 4)
worker = worker.groupby('department').agg(count(worker['worker_id']).alias('num_workers')).sort('num_workers' , ascending = False)

# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()
# department	num_workers
# Admin	        4
# HR        	1


