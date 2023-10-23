# link   : https://platform.stratascratch.com/coding/2056-number-of-shipments-per-month?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
from pyspark.sql.functions import *
# Start writing code
amazon_shipment = amazon_shipment.groupby(date_format(amazon_shipment['shipment_date'] , 'yyyy-MM')).agg(count('shipment_id'))

# To validate your solution, convert your final pySpark df to a pandas df
amazon_shipment.toPandas()

