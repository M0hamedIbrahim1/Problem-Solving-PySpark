# link   : https://platform.stratascratch.com/coding/10046-top-5-states-with-5-star-businesses?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
# Start writing code

result = (
    yelp_business.filter(yelp_business.stars == 5)
    .groupBy("state")
    .agg(F.count("business_id").alias("n_businesses"))
    .withColumn(
        "rank", F.rank().over(Window.orderBy(F.desc("n_businesses")))
    )
    .filter(F.col("rank") <= 5)
    .select("state", "n_businesses")
    .orderBy(F.desc("n_businesses"), "state")
    .toPandas()
)
