# link   : https://platform.stratascratch.com/coding/9688-churro-activity-date?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark

# Start writing code
transformation_1 = los_angeles_restaurant_health_inspections
transformation_1 = transformation_1.filter(transformation_1['facility_name'] == 'STREET CHURROS').select('activity_date' , 'pe_description')
# To validate your solution, convert your final pySpark df to a pandas df
transformation_1.toPandas()

