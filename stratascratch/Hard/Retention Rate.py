# link   : https://platform.stratascratch.com/coding/2053-retention-rate?code_type=5
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Start writing code
sf_events.createOrReplaceTempView('sf_events')
res = spark.sql(
    """
    with cte_dec as
    (
        select account_id, count(user_id) as dec 
        from sf_events
        where user_id in( Select user_id from sf_events where year(date)='2020'and month(date)='12')
        group by  account_id
    ),
    cte_jan as 
    (
      
        select account_id, count(user_id) as jan 
        from sf_events
        where user_id in(Select user_id from sf_events where year(date)='2021'and month(date)='1')
        group by  account_id
    )
    
    select t1.account_id,floor(t1.jan/t2.dec) retention 
    from cte_dec t2
    left join cte_jan t1 
    ON t1.account_id = t2.account_id
       """
)

# To validate your solution, convert your final pySpark df to a pandas df
res.toPandas()

