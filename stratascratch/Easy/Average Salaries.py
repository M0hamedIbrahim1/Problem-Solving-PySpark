# link   : https://platform.stratascratch.com/coding/9917-average-salaries?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import window

# Start writing code
employee = employee.withColumn('avg_per_Dept' , avg('salary').over(Window.partitionBy('department')))
employee = employee.select('department','first_name','salary','avg_per_Dept')
# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()

