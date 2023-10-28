# link   : https://platform.stratascratch.com/coding/9610-find-students-with-a-median-writing-score?code_type=6
# author : Mohamed Ibrahim 

# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# Start writing code

sat_scores =( sat_scores 
            .withColumn('rnk1', row_number().over(Window.orderBy('sat_writing')))
            .withColumn('rnk2', row_number().over(Window.orderBy(desc('sat_writing'))))
            )

Trans_1 = sat_scores.filter((col('rnk1') >= col('rnk2') - 1) & (col('rnk1') <= col('rnk2')+1))
result = Trans_1.select('student_id')
# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()

