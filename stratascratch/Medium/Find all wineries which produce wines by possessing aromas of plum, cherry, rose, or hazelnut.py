# link   : https://platform.stratascratch.com/coding/10026-find-all-wineries-which-produce-wines-by-possessing-aromas-of-plum-cherry-rose-or-hazelnut?code_type=6
# author : Mohamed Ibrahim
# Import your libraries
import pyspark
from pyspark.sql.functions import* 

# Start writing code
winemag_p1.filter((lower(col("description")).rlike('plum[^a-z]'))|
                           (lower(col("description")).rlike('cherry[^a-z]'))|
                           (lower(col("description")).rlike('rose[^a-z]'))|
                           (lower(col("description")).rlike('hazelnut[^a-z]'))
                            ).select(col("winery")).distinct().toPandas()

