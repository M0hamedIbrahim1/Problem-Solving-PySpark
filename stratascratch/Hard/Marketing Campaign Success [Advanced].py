# link   : https://platform.stratascratch.com/coding/514-marketing-campaign-success-advanced?code_type=3
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import countDistinct, dense_rank, row_number, col

# Start writing code
window_spec_1 = Window.partitionBy("user_id").orderBy("created_at")
window_spec_2 = Window.partitionBy("user_id" , "product_id").orderBy("created_at")

a = marketing_campaign.withColumn("date_rank", dense_rank().over(window_spec_1)) \
    .withColumn("prod_rank", row_number().over(window_spec_2))

result = a.filter((col("date_rank") > 1) & (col("prod_rank") == 1)) \
    .agg(countDistinct("user_id").alias("cnt"))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()

