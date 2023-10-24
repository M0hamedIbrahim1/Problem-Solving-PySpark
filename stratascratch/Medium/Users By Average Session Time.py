# link   : platform.stratascratch.com/coding/10352-users-by-avg-session-time?code_type=5
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Start writing code
facebook_web_log.createOrReplaceTempView('facebook_web_log')
spark.sql(
    
    """with cte as (
    select user_id
    , datediff(second, max(case when action = 'page_load' then timestamp end), min(case when action = 'page_exit' then timestamp end)) as session_time
    from facebook_web_log
    group by user_id, cast(timestamp as date)
    )
    select user_id, round(sum(session_time)*1.0/count(session_time),1) as session_time
    from cte
    where session_time is not null
    group by user_id    """
    
).toPandas()

