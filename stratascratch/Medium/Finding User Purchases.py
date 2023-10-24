# link   : https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=6
# author : Mohamed ibrahim


# Import your libraries
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window
# Start writing code
df = amazon_transactions.orderBy(F.col("user_id"), F.col("created_at"))
df = df.withColumn("prev_value", F.lag("created_at").over(Window.partitionBy("user_id").orderBy("created_at")))
df = df.withColumn("days", F.datediff(F.col("created_at"), F.col("prev_value")))
result = df.filter(F.col("days") <= 7).select("user_id").distinct()
# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
