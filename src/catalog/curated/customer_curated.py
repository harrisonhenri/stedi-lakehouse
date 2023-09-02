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

# Script generated for node Customer Table
CustomerTable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-health-analytics",
    table_name="customer_trusted",
    transformation_ctx="CustomerTable_node1",
)

# Script generated for node Accelerometer Table
AccelerometerTable_node1693612185576 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-health-analytics",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTable_node1693612185576",
)

# Script generated for node Join
Join_node1693612346522 = Join.apply(
    frame1=CustomerTable_node1,
    frame2=AccelerometerTable_node1693612185576,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693612346522",
)

# Script generated for node Drop Fields
DropFields_node1693612400043 = DropFields.apply(
    frame=Join_node1693612346522,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1693612400043",
)

# Script generated for node Customer Curated
CustomerCurated_node1693612460007 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1693612400043,
    database="stedi-health-analytics",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1693612460007",
)

job.commit()
