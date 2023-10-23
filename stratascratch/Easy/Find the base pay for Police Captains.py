# link   : https://platform.stratascratch.com/coding/9972-find-the-base-pay-for-police-captains?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark

# Start writing code
Trans_1 = sf_public_salaries.filter(sf_public_salaries['jobtitle'].like('CAPTAIN III (POLICE DEPARTMENT)'))
Trans_2 = Trans_1.select('employeename', 'basepay')
# To validate your solution, convert your final pySpark df to a pandas df
Trans_2.toPandas()

