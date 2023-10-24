# link   : https://platform.stratascratch.com/coding/10351-activity-rank?code_type=5
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code
result = google_gmail_emails.groupby('from_user').agg(F.count('*').alias('total_emails'))
result = result.withColumn('rank', F.row_number().over(Window.orderBy(F.desc('total_emails'), 'from_user')))
result = result.orderBy(F.desc('total_emails'), 'from_user')

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()


