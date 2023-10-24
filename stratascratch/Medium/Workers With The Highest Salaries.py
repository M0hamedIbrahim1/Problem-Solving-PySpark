# link   : https://platform.stratascratch.com/coding/10353-workers-with-the-highest-salaries?code_type=6
# author : Mohamed Ibrahim

import pyspark
from pyspark.sql.functions import *
from pyspark.sql import Window

car_launches = car_launches.groupby('company_name','year').agg(count('product_name').alias('cnt'))
window = Window.partitionBy('company_name').orderBy('year')
car_launches = car_launches.withColumn('past_year' , lag(car_launches['cnt']).over(window))
car_launches = car_launches.withColumn('diff' ,car_launches['cnt'] - car_launches['past_year'] )
car_launches = car_launches.filter(car_launches['diff'].isNotNull()).select('company_name' ,'diff' )
car_launches.toPandas()



# using SQL : 

# with cte as (
# select company_name,year,count(*)as cnt from car_launches
# group by company_name,year
# ),
# cte_2 as (
#     select company_name , cnt - lag(cnt)over(partition by company_name order by year) net_products from cte
# )

# select company_name , net_products from cte_2
# where net_products is not null

