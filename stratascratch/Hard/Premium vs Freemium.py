# link   : https://platform.stratascratch.com/coding/10300-premium-vs-freemium?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import sum, col
# Start writing code
df1 = ms_download_facts.join(
    ms_user_dimension, ms_download_facts["user_id"] == ms_user_dimension["user_id"]
).join(
    ms_acc_dimension, ms_user_dimension["acc_id"] == ms_acc_dimension["acc_id"]
).groupBy(ms_download_facts["date"], ms_acc_dimension["paying_customer"]) \
.agg(sum(ms_download_facts["downloads"]).alias("downloads"))

df2 = df1.alias("t1").join(
    df1.alias("t2"),
    (col("t1.date") == col("t2.date")) &
    (col("t1.paying_customer") == 'no') &
    (col("t2.paying_customer") == 'yes') &
    (col("t1.downloads") > col("t2.downloads"))
).select(col("t1.date"), col("t1.downloads").alias("Nodownloads"), col("t2.downloads").alias("Yesdownloads"))


# To validate your solution, convert your final pySpark df to a pandas df
df2.toPandas()
