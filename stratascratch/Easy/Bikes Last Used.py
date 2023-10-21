# link   : https://platform.stratascratch.com/coding/10176-bikes-last-used?code_type=6
# author : Mohamed Ibrahim


# Import your libraries
from pyspark.sql.functions import *

# Start writing code
dc_bikeshare_q1_2012 = dc_bikeshare_q1_2012.groupby('bike_number').agg(max('end_time').alias('last_used')).sort('last_used' , ascending = False)

# To validate your solution, convert your final pySpark df to a pandas df
dc_bikeshare_q1_2012.toPandas()





