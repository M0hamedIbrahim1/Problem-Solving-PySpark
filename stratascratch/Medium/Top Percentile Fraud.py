# link   : https://platform.stratascratch.com/coding/10303-top-percentile-fraud?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

fraud_score.createOrReplaceTempView('fraud_score')

# Start writing code
spark.sql('''select policy_num, state, claim_cost, fraud_score from
(select *, ntile(20) over(partition by state order by fraud_score desc) as top5 from fraud_score) b
where top5=1
order by 2,1''').toPandas()

# To validate your solution, convert your final pySpark df to a pandas df
# fraud_score.toPandas()
