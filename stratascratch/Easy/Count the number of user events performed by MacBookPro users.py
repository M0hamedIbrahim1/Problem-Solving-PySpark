# link   : https://platform.stratascratch.com/coding/9653-count-the-number-of-user-events-performed-by-macbookpro-users?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
playbook_events = playbook_events.filter(playbook_events['device'] == 'macbook pro')
playbook_events = (
    playbook_events
    .groupby('event_name')
    .agg(count('user_id').alias('event_count'))
    .sort('event_count' , ascending = False)
)

# To validate your solution, convert your final pySpark df to a pandas df
playbook_events.toPandas()


