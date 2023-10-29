# link   : https://platform.stratascratch.com/coding/9781-find-the-rate-of-processed-tickets-for-each-type?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
t1 = facebook_complaints.groupby('type').agg(sum(when(col('processed') == 'TRUE' , 1).otherwise(0)).alias('sm_True') , count('*').alias('cnt'))
t2 = t1.select('type' ,col('sm_True') / col('cnt') )
# To validate your solution, convert your final pySpark df to a pandas df
t2.toPandas()

