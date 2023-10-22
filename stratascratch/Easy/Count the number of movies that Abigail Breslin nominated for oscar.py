# link  : https://platform.stratascratch.com/coding/10128-count-the-number-of-movies-that-abigail-breslin-nominated-for-oscar?code_type=6
# auhor : Mohamed Ibrahim

# Import your libraries
import pyspark

# Start writing code
oscar_nominees = oscar_nominees.filter(oscar_nominees['nominee'] == 'Abigail Breslin')
oscar_nominees = oscar_nominees.select('movie').distinct().count() 
# To validate your solution, convert your final pySpark df to a pandas df
# oscar_nominees.toPandas()
