# link   : https://platform.stratascratch.com/coding/2029-the-most-popular-client_id-among-users-using-video-and-voice-calls?code_type=5
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import *
# Start writing code
fact_events = fact_events.filter((col('event_type')=='video call received') | (col('event_type')=='video call sent') | (col('event_type')== 'voice call received') | (col('event_type')=='voice call sent'))

fact_events = fact_events.groupBy('client_id').agg(count('*').alias('cnt'))
windowspec = Window.orderBy(col('cnt').desc())
fact_events = fact_events.withColumn('rnk',dense_rank().over(windowspec))
fact_events = fact_events.select(col('client_id')).filter(col('rnk')==1)
# To validate your solution, convert your final pySpark df to a pandas df
fact_events.toPandas()
