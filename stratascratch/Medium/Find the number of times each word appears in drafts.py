# link   : https://platform.stratascratch.com/coding/9817-find-the-number-of-times-each-word-appears-in-drafts?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
Trans_1 = google_file_store.filter(col('filename').like('%draft%'))
Trans_2 = Trans_1.select('filename' , regexp_replace('contents' , '[.,]' , '').alias('string'))
Trans_3 = Trans_2.select('filename' , split('string' , ' ').alias('words'))
Trans_4 = Trans_3.select('filename' , explode('words').alias('words')).groupby('words').agg(count('*').alias('cnt')).orderBy(desc('cnt'))

# To validate your solution, convert your final pySpark df to a pandas df
Trans_4.toPandas()
