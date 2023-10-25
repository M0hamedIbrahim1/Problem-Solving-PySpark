# link   : https://platform.stratascratch.com/coding/10064-highest-energy-consumption?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
eu=fb_eu_energy
asia=fb_asia_energy
na=fb_na_energy
# To validate your solution, convert your final pySpark df to a pandas df
# fb_eu_energy.toPandas()

df=eu.union(asia).union(na)
# df.show()÷
df.groupBy('date').sum('consumption').orderBy('date',ascending=False).limit(2).toPandas()
