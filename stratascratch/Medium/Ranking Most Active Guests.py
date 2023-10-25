# link   : https://platform.stratascratch.com/coding/10159-ranking-most-active-guests?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# Start writing code
airbnb_contacts = airbnb_contacts.groupby('id_guest').agg(sum('n_messages').alias('nn_messages'))
airbnb_contacts = airbnb_contacts.withColumn('rnk' , dense_rank().over(Window.orderBy(desc(col('nn_messages')))))
airbnb_contacts = airbnb_contacts.select('id_guest' ,'nn_messages','rnk')
# To validate your solution, convert your final pySpark df to a pandas df
airbnb_contacts.toPandas()


