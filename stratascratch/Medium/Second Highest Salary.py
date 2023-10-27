# link   : https://platform.stratascratch.com/coding/9892-second-highest-salary?code_type=6
# author : Mohamed Ibrahim

import pyspark.sql.functions as F
from pyspark.sql.window import Window

employee = employee.withColumn('rnk', F.dense_rank().over(Window.orderBy(F.desc('salary'))))
result = employee.filter(F.col('rnk') == 2).select('salary')

result.toPandas()

