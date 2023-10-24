# link : https://platform.stratascratch.com/coding/10304-risky-projects?code_type=6
# author : Mohamed Ibrahim

# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Start writing code
linkedin_employees = linkedin_employees.withColumn("per_day_sal", F.col("salary")/365)
emp_projects = linkedin_emp_projects.alias("t1").join(linkedin_employees.alias("t2"), F.col("t1.emp_id") == F.col("t2.id"), "left")
emp_projects = emp_projects.groupBy("project_id").agg(F.sum("per_day_sal").alias("emp_sal"))

res = linkedin_projects.join(emp_projects, linkedin_projects.id == emp_projects.project_id, "left")

res = res.withColumn("prorated_expense", F.ceil(F.datediff(F.col("end_date"), F.col("start_date"))*F.col("emp_sal"))).filter("prorated_expense > budget").select("title", "budget", "prorated_expense").orderBy("title")

res.toPandas()
