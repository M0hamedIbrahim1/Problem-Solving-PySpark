# link   : https://platform.stratascratch.com/coding/9905-highest-target-under-manager?code_type=6
# author : Mohamed Ibrahim

from pyspark.sql.functions import *

df = salesforce_employees.filter(salesforce_employees['manager_id'] == 13).select('first_name', 'target')
result = df.filter(df['target'] == df.selectExpr('max(target)').collect()[0][0]).toPandas()
