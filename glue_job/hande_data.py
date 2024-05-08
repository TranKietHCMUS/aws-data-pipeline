import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_date
import pandas as pd
import datetime

from joblib import variables as V

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

today = datetime.date.today()

a_day = datetime.timedelta(days = 1)

yesterday = today - a_day

raw_today_df = spark.read.csv(f's3://ai4e-ap-southeast-1-dev-s3-data-landing/kiettna/raw/comic_{today.day}-{today.month}-{today.year}.csv', header=True, inferSchema=True).cache()
raw_today_df.createOrReplaceTempView("raw_today")


bucket_name = "ai4e-ap-southeast-1-dev-s3-data-landing"
object_key = "kiettna/golden_zone"



total_view_today_query = """
select count(*) as num_of_story, sum(view) as total_view
from raw_today
"""

total_view_today_df = spark.sql(total_view_today_query)
total_view_today_df = total_view_today_df\
    .withColumn("date", current_date())


view_of_type_query = """
select story_type, count(*) as num_of_story, sum(view) as total_view
from raw_today
group by story_type
"""

view_of_type_df = spark.sql(view_of_type_query)
view_of_type_df = view_of_type_df\
    .withColumn("date", current_date())



if (str(today) == "2024-05-05"):
    total_view_today_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/{object_key}/total_view')
    total_view_today_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/golden_zone/kiettna_account_data/total_view')

    view_of_type_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/{object_key}/view_of_type')
    view_of_type_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/golden_zone/kiettna_account_data/view_of_type')

    new_story_query = """
    select story_name, story_link, author, story_type 
    from raw_today
    """

    new_story_df = spark.sql(new_story_query)

    new_story_df = new_story_df\
        .withColumn("date", current_date())

    new_story_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/{object_key}/new_story')
    new_story_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/golden_zone/kiettna_account_data/new_story')
else:
    total_view_yesterday_df = spark.read.parquet(f's3://{bucket_name}/{object_key}/total_view')
    total_view_today_df.createOrReplaceTempView("view_today")
    total_view_yesterday_df.createOrReplaceTempView("view_yesterday")

    total_view_query = """
    select * from view_today
    union
    select * from view_yesterday
    """

    total_view_df = spark.sql(total_view_query)

    total_view_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/{object_key}/total_view')
    total_view_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/golden_zone/kiettna_account_data/total_view')

    # --------------------------------------------------------------------------

    old_view_of_type_df = spark.read.parquet(f's3://{bucket_name}/{object_key}/view_of_type')
    view_of_type_df.createOrReplaceTempView("view_of_type")
    old_view_of_type_df.createOrReplaceTempView("old_view_of_type")

    view_of_type_query = """
    select * from view_of_type
    union
    select * from old_view_of_type
    """

    new_view_of_type_df = spark.sql(view_of_type_query)

    new_view_of_type_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/{object_key}/view_of_type')
    new_view_of_type_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/golden_zone/kiettna_account_data/view_of_type')

    # ----------------------------------------------------------------------------

    raw_yesterday_df = spark.read.csv(f's3://ai4e-ap-southeast-1-dev-s3-data-landing/kiettna/raw/comic_{yesterday.day}-{yesterday.month}-{yesterday.year}.csv', header=True, inferSchema=True).cache()
    raw_yesterday_df.createOrReplaceTempView("raw_yesterday")

    temp_query = """
    select story_name, story_link, author, story_type
    from raw_today
    except
    select story_name, story_link, author, story_type
    from raw_yesterday
    """

    temp_df = spark.sql(temp_query)
    temp_df = temp_df\
        .withColumn("date", current_date())
    temp_df.createOrReplaceTempView("temp1")

    new_story_yesterday_df = spark.read.parquet(f's3://{bucket_name}/{object_key}/new_story')
    new_story_yesterday_df.createOrReplaceTempView("temp2")

    new_story_query = """
    select * from temp1
    union
    select * from temp2
    """

    new_story_df = spark.sql(new_story_query)

    new_story_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/{object_key}/new_story')
    new_story_df.coalesce(1).write.mode('overwrite').parquet(f's3://{bucket_name}/golden_zone/kiettna_account_data/new_story')
