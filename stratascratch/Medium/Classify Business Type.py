# link   : https://platform.stratascratch.com/coding/9726-classify-business-type?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
                    
data = sf_restaurant_health_violations.withColumn('business_type',
    when(lower(col('business_name')).like('%restaurant%'), 'restaurant')
    .when(lower(col('business_name')).like('%café%'), 'cafe')
    .when(lower(col('business_name')).like('%cafe%'), 'cafe')
    .when(lower(col('business_name')).like('%coffee%'), 'cafe')
    .when(lower(col('business_name')).like('%school%'), 'school')
    .otherwise('other')
)                 
data = data.select('business_name' , 'business_type').dropDuplicates()


# To validate your solution, convert your final pySpark df to a pandas df
data.toPandas()
