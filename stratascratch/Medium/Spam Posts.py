# link   : https://platform.stratascratch.com/coding/10134-spam-posts?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
joined_df = facebook_posts.join(facebook_post_views, "post_id")

# Calculate the spam ratio for each post_date
result = joined_df.groupBy("post_date").agg(
    (sum(when(joined_df["post_keywords"].like("%spam%"), 1).otherwise(0)) * 100 / count("*")).alias("spam_ratio")
)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()


#SQL:
# select post_date, sum(case when post_keywords LIKE '%spam%' THEN 1 else 0 end)*100/count(*)
# FROM facebook_posts a 
# JOIN facebook_post_views b on a.post_id = b.post_id
# GROUP BY 1
