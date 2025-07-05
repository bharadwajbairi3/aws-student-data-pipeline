import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3

# Specify your S3 bucket and path
bucket_name = 'bb-de-project-2'
prefix = 'datawarehouse/'

# Initialize S3 client
s3 = boto3.client('s3')


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    if any(frame.count() == 0 for alias, frame in mapping.items()):
        non_empty_frame = next(frame for alias, frame in mapping.items() if frame.count() > 0)
        return non_empty_frame
    else:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql("(select * from {}) UNION {} (select * from {})".format(*mapping.keys()))
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read Marks Data
marks_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://bb-de-project-2/staging/marks/"], "recurse": True},
    transformation_ctx="marks_df",
)
print("✔ Read Marks Data")

# Read Student Data
students_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://bb-de-project-2/staging/student/"], "recurse": True},
    transformation_ctx="students_df",
)
print("✔ Read Students Data")

# Delete old files before reading warehouse to avoid corrupted union
try:
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print("✔ Deleted existing files in warehouse/")
    else:
        print("ℹ No existing files to delete.")
except Exception as e:
    print(f"⚠ Error while deleting old files: {e}")

# Create an empty DW dynamic frame to avoid union issue
from awsglue.dynamicframe import DynamicFrame
empty_df = spark.createDataFrame([], marks_df.toDF().schema)
dw_df = DynamicFrame.fromDF(empty_df, glueContext, "dw_df")

# Join Marks and Students
joined_df = Join.apply(
    frame1=marks_df,
    frame2=students_df,
    keys1=["student_id"],
    keys2=["id"],
    transformation_ctx="joined_df",
)
print("✔ Join Successful")

# Union Joined with empty DW
union_df = sparkUnion(
    glueContext,
    unionType="ALL",
    mapping={"source1": joined_df, "source2": dw_df},
    transformation_ctx="union_df",
)
print("✔ Union Successful")

# Write the output to S3 in Parquet format
output_path = f"s3://{bucket_name}/{prefix}"
output = glueContext.write_dynamic_frame.from_options(
    frame=union_df,
    connection_type="s3",
    format="parquet",
    connection_options={"path": output_path, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="output",
)
print("✔ Data Saved to Warehouse")

job.commit()
print("✔ Glue Job Completed")
