# link   : https://platform.stratascratch.com/coding/10319-monthly-percentage-difference?code_type=6
# author : Mohamed Ibrahim


# Import your libraries
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import *

cte = sf_transactions.groupBy(
    date_format(col("created_at"), "yyyy-MM").alias("month")
).agg(sum("value").alias("sm"))

window_spec = Window.orderBy("month")

cte_2 = cte.withColumn("pst_month_value", lag("sm").over(window_spec))

result = cte_2.withColumn("percentage_change", (100 * (col("sm") - col("pst_month_value")) / col("pst_month_value")))

result = result.select("month", "percentage_change")

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()

