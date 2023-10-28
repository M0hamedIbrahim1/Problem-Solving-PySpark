# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code

user_flags.filter("flag_id is not NULL") \
.select("video_id", 
    (when(col("user_firstname").isNull(), "unknown").otherwise(col("user_firstname")).alias("user_firstname")), 
    (when(col("user_lastname").isNull(), "unknown").otherwise(col("user_lastname")).alias("user_lastname"))
    ).groupBy("video_id").agg(countDistinct(concat(col("user_firstname"),col("user_lastname"))).alias("num_unique_users")).toPandas()
