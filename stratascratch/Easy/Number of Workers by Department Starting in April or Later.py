# link  : https://platform.stratascratch.com/coding/9847-find-the-number-of-workers-by-department?code_type=6
# author : Mohamed Ibrahim


# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
worker = worker.filter((month(worker['joining_date']) >= 4) & (worker['department'] == 'Admin'))
worker = worker.select('worker_id').count()

