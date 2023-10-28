# link   : https://platform.stratascratch.com/coding/9728-inspections-that-resulted-in-violations/solutions?code_type=6
# author : Mohamed Ibrahim

import pyspark.sql.functions as F

df = sf_restaurant_health_violations.filter(F.col('business_name')=='Roxanne Cafe')
df = df.withColumn('year', F.year('inspection_date'))
df = df.groupBy('year').agg(F.count('violation_id').alias('violation_count')).orderBy(F.asc('violation_count')).toPandas()
