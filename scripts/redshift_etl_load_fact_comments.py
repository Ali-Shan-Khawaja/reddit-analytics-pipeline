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
    job_name = "Fact_Comments_Load_Job"
    
    
# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)
# S3 path to dim_user parquet
parquet_path = "s3://<s3_dir>/reddit-dw/fact_comment/"

# Load parquet as DataFrame
df = spark.read.parquet(parquet_path)
df.printSchema()
df.show(5)
# Load dim_user and dim_subreddit tables from Redshift
redshift_options = {
    "url": "jdbc:redshift://<cluster_id>.redshift.amazonaws.com:5439/dev",
    "user": "<db_user>",
    "password": "<db_pass>",
    "redshiftTmpDir": "s3://<s3_dir>/temporary/",
    "aws_iam_role": "arn:aws:iam::<redshift_role_with_glue_s3_access>"
}
dim_user_df = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={**redshift_options, "dbtable": "reddit_dw.dim_user"}
).toDF()
dim_sub_df = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={**redshift_options, "dbtable": "reddit_dw.dim_subreddit"}
).toDF()
# Join comments with dim_user on author = subreddit_user
comments_with_user = df.join(dim_user_df, df["author"] == dim_user_df["author"], "left")

# Join comments with dim_subreddit on subreddit_id
final_df = comments_with_user.join(dim_sub_df, comments_with_user["subreddit_id"] == dim_sub_df["subreddit_id"], "left")

# Select and rename columns to match fact_comments table schema
final_df_cleaned = final_df.select(
    df["comment_id"],
    dim_user_df["user_id"],
    dim_sub_df["subreddit_key"].alias("subreddit_id"),
    df["created_datetime"],
    df["score"],
    df["ups"],
    df["downs"],
    df["controversiality"],
    df["gilded"]
)
# Convert to DynamicFrame
final_dyf = DynamicFrame.fromDF(final_df_cleaned, glueContext, "final_dyf")
# Write to fact_comments table in Redshift
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="redshift",
    connection_options={
        **redshift_options,
        "dbtable": "reddit_dw.fact_comments",
        "preactions": "TRUNCATE TABLE reddit_dw.fact_comments"
    }
)
#Stop Spark
spark.stop()
# Commit job
job.commit()