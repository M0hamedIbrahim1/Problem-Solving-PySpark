# link   : https://platform.stratascratch.com/coding/2024-unique-users-per-client-per-month?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
fact_events = fact_events.groupby('client_id',month('time_id')).agg(countDistinct('user_id'))

# To validate your solution, convert your final pySpark df to a pandas df
fact_events.toPandas()
