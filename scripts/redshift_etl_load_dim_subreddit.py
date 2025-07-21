import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
# Fallback for local/dev testing
args = {}
if '--JOB_NAME' in sys.argv:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']
else:
    job_name = "default_job"
    
    
# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)
# S3 path to dim_user parquet
parquet_path = "s3://<se_dir>/reddit-dw/dim_subreddit/"

# Load parquet as DataFrame
df = spark.read.parquet(parquet_path)
# Optional: verify schema
df.printSchema()
df.show()
# Convert DataFrame to DynamicFrame
dyf = DynamicFrame.fromDF(df, glueContext, "dyf")


df_clean = dyf.toDF().select("subreddit_id", "subreddit", "subreddit_key")
dyf_clean = DynamicFrame.fromDF(df_clean, glueContext, "dyf_clean")

dyf_clean.printSchema()

# Write to Redshift
df_clean.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", "jdbc:redshift://<cluster_id>.redshift.amazonaws.com:5439/dev?user=<db_user>&password=<db_pass>") \
    .option("dbtable", "reddit_dw.dim_subreddit") \
    .option("tempdir", "s3://<s3_dir>/temporary/") \
    .option("aws_iam_role", "arn:aws:iam::<redshift_role_with_glue_s3_access>") \
    .mode("overwrite") \
    .save()

#Stop Spark
spark.stop()
# Commit job
job.commit()