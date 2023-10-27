# link   : https://platform.stratascratch.com/coding/9897-highest-salary-in-department?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Start writing code
employee_T1 = employee.withColumn('rnk' , dense_rank().over(Window.partitionBy('department').orderBy(desc('salary')) ))
employee_T2 = employee_T1.filter(col('rnk') == 1).select('department' , 'first_name' , 'salary')

# To validate your solution, convert your final pySpark df to a pandas df
employee_T2.toPandas()


