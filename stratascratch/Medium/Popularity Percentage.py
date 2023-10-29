# author : Mohamed Ibrahim
# link   : https://platform.stratascratch.com/coding/10284-popularity-percentage?code_type=6

# Import your libraries
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Start writing code
facebook_friends.createOrReplaceTempView('facebook_friends')
res = spark.sql(
    """with cte as
    (select user1 , user2 from facebook_friends
    union 
    select user2 , user1 from facebook_friends
    ),cnt_friends as (
        select user1 , count(*) as cnt from cte
        group by user1
    )
    select user1 , (cnt*100.0 / (select count(distinct user1) from cte)) popularity_percent
    from cnt_friends
    order by popularity_percent desc , user1"""
    
)


# To validate your solution, convert your final pySpark df to a pandas df
res.toPandas()
