# link   : https://platform.stratascratch.com/coding/9632-host-popularity-rental-prices?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
from pyspark.sql.functions import col, concat, when, min, max, avg
from pyspark.sql.window import Window

# Start writing code
results = airbnb_host_searches.withColumn(
    "id",
    concat(
        col("price"),
        col("room_type"),
        col("host_since"),
        col("zipcode"),
        col("number_of_reviews")
    )
).distinct().withColumn(
    "popularity",
    when(col("number_of_reviews") == 0, "New")\
    .when(col("number_of_reviews").between(1, 5), "Rising")\
    .when(col("number_of_reviews").between(6, 15), "Trending Up")\
    .when(col("number_of_reviews").between(16, 40), "Popular")\
    .otherwise("Hot")
).groupBy("popularity").agg(
    min(col("price")).alias("min_price"),
    avg(col("price")).alias("avg_price"),
    max(col("price")).alias("max_price")
)

# To validate your solution, convert your final pySpark df to a pandas df
results.toPandas()
