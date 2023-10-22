# link   : https://platform.stratascratch.com/coding/9663-find-the-most-profitable-company-in-the-financial-sector-of-the-entire-world-along-with-its-continent?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
forbes_global_2010_2014 = (
                            forbes_global_2010_2014
                            .groupby('company' , 'continent')
                            .agg(max('profits'))
                            .sort(max('profits') , ascending = False)
                            .limit(1)
                            .select('company' , 'continent')
                            )

# To validate your solution, convert your final pySpark df to a pandas df
forbes_global_2010_2014.toPandas()
