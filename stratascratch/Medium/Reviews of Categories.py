# link   : https://platform.stratascratch.com/coding/10049-reviews-of-categories?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
import pyspark.sql.functions as F

# Start writing code

result = (yelp_business
    .select("categories", "name", "review_count")
    .withColumn("categories", F.split("categories", ";"))
    .withColumn("categories", F.explode("categories"))
    .groupBy("categories").agg(F.sum("review_count").alias("total_reviews"))
    .orderBy(F.desc("total_reviews"))
    .select("categories", "total_reviews")
)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
