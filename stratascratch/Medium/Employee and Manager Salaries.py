# link   : https://platform.stratascratch.com/coding/9894-employee-and-manager-salaries?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code

merged = employee.alias("e1").join(employee.alias("e2"), (col("e1.manager_id") == col("e2.id")) & (col("e1.salary") > col("e2.salary")) ,'inner')
merged = merged.select('e1.first_name' , 'e1.salary')
# To validate your solution, convert your final pySpark df to a pandas df
merged.toPandas()


