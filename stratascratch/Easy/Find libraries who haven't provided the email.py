# link   : https://platform.stratascratch.com/coding/9924-find-libraries-who-havent-provided-the-email-address-in-2016-but-their-notice-preference-definition-is-set-to-email?code_type=6
# author : Mohamed Ibrahim

import pyspark.sql.functions as F

Transformation_1 = library_usage.filter(
    (library_usage["notice_preference_definition"] == "email")
    & (library_usage["provided_email_address"] == False)
    & (library_usage["circulation_active_year"] == 2016)
)

Transformation_1.select("home_library_code").distinct().toPandas()
