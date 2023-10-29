# link   : https://platform.stratascratch.com/coding/2099-election-results?code_type=5
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Start writing code
vote_cnt = voting_results.filter(col("candidate").isNotNull()) \
    .withColumn("vote_count", count("candidate").over(Window.partitionBy("voter")))

process = vote_cnt.select(col("candidate"), (1.0 / col("vote_count")).alias("point"))

result = process.groupBy("candidate").agg(sum("point").alias("votess"))

result = result.withColumn("rank", row_number().over(Window.orderBy(col("votess").desc())))
top_candidate = result.filter(col("rank") == 1).select("candidate")


# To validate your solution, convert your final pySpark df to a pandas df
top_candidate.toPandas()
