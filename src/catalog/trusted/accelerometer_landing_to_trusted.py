import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Bucket
CustomerBucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-health-analytics-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerBucket_node1",
)

# Script generated for node Accelerometer Bucket
AccelerometerBucket_node1693583271609 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-health-analytics-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerBucket_node1693583271609",
)

# Script generated for node Join
Join_node1693583245482 = Join.apply(
    frame1=CustomerBucket_node1,
    frame2=AccelerometerBucket_node1693583271609,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693583245482",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1693583729990 = glueContext.write_dynamic_frame.from_catalog(
    frame=Join_node1693583245482,
    database="stedi-health-analytics",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1693583729990",
)

job.commit()
