# link   : https://platform.stratascratch.com/coding/10077-income-by-title-and-gender?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
total_bonus = (sf_bonus 
            .groupBy('worker_ref_id') 
            .agg(sum('bonus').alias('bonus'))
            )

result = (
        sf_employee 
        .join(total_bonus, sf_employee.id == total_bonus.worker_ref_id, how='left') 
        .groupBy('employee_title', 'sex') 
        .agg(avg(col('salary') + col('bonus')).alias('avg_compensation'))
        )
# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()

