# link   : https://platform.stratascratch.com/coding/10166-reviews-of-hotel-arena?code_type=6
# author : Mohamed Ibrahim


import pyspark
from pyspark.sql.functions import *

arena = hotel_reviews.filter(hotel_reviews['hotel_name'] == 'Hotel Arena')
result = arena.groupby(['reviewer_score','hotel_name']).agg(count('*').alias('n_reviews'))
result.toPandas()
