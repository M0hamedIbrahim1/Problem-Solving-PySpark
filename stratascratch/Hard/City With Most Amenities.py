# link   : https://platform.stratascratch.com/coding/9633-city-with-most-amenities?code_type=6
# author : Mohamed Ibrahim

import pyspark.sql.functions as F

df = airbnb_search_details.withColumn('amenities_count', F.size(F.split(F.col('amenities'), ',')))
result = df.groupBy('city').agg(F.sum('amenities_count').alias('amenities_count')).orderBy(F.desc('amenities_count')).limit(1)
result = result.select('city').toPandas()

