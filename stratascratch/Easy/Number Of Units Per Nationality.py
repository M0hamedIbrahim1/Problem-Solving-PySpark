# link   : https://platform.stratascratch.com/coding/10156-number-of-units-per-nationality?code_type=6
# Author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
airbnb_hosts = 
            (
              airbnb_hosts.join(airbnb_units, on='host_id', how='inner')
              .filter((F.col('age')<30) & (F.col('unit_type') == 'Apartment'))
              .groupBy(F.col('nationality')).agg(F.countDistinct('city'))
            )

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_hosts.toPandas()
