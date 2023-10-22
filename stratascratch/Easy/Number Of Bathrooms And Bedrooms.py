# link   : https://platform.stratascratch.com/coding/9622-number-of-bathrooms-and-bedrooms?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
airbnb_search_details = (
                        airbnb_search_details
                        .groupby('city' , 'property_type')
                        .agg(
                                mean('bedrooms').alias('n_bedrooms_avg'),
                                mean('bathrooms').alias('n_bathrooms_avg')
                            )
                        )

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_search_details.toPandas()
# city	property_type	n_bedrooms_avg	n_bathrooms_avg

