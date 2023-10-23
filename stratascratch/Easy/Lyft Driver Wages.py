# link   : https://platform.stratascratch.com/coding/10003-lyft-driver-wages?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark

# Start writing code
lyft_drivers = lyft_drivers.filter((lyft_drivers['yearly_salary']>= 70000) | (lyft_drivers['yearly_salary'] <= 30000))

# To validate your solution, convert your final pySpark df to a pandas df
lyft_drivers.toPandas()

