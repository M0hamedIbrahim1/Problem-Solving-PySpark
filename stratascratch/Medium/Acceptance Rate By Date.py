# link   : https://platform.stratascratch.com/coding/10285-acceptance-rate-by-date?code_type=6
# author : Mohamed Ibrahim

import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
fb_friend_requests.createOrReplaceTempView('fb_friend_requests')
spark.sql(

"""
with cte as 
(
    select user_id_sender , user_id_receiver , min(date) as date from fb_friend_requests
    group by user_id_sender , user_id_receiver
    having count(*) > 1
),
ACC_DATA as (
    select date , count(*) accept from cte 
    group by date
),
SENT_DATA as (
    select date , sum(case when action = 'sent' then 1 else 0 end) as sent 
    from fb_friend_requests
    group by date
)
select t1.date , t1.accept*1.0 / t2.sent 
from ACC_DATA t1
left join SENT_DATA t2 
on t1.date = t2.date

"""
    
).toPandas()

