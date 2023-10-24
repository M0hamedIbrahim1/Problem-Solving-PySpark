# link   : https://platform.stratascratch.com/coding/10354-most-profitable-companies?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
forbes_global_2010_2014 = forbes_global_2010_2014.select('company' , 'profits').orderBy(desc('profits')).limit(3)

# To validate your solution, convert your final pySpark df to a pandas df
forbes_global_2010_2014.toPandas()
