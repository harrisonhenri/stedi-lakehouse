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

# Script generated for node Step Trainer Bucket
StepTrainerBucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-health-analytics-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerBucket_node1",
)

# Script generated for node Customer Table
CustomerTable_node1693613631309 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-health-analytics",
    table_name="customer_curated",
    transformation_ctx="CustomerTable_node1693613631309",
)

# Script generated for node Join
Join_node1693613644272 = Join.apply(
    frame1=StepTrainerBucket_node1,
    frame2=CustomerTable_node1693613631309,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1693613644272",
)

# Script generated for node Drop Fields
DropFields_node1693613738183 = DropFields.apply(
    frame=Join_node1693613644272,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1693613738183",
)

# Script generated for node Step Trainer Table
StepTrainerTable_node1693613652148 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1693613738183,
    database="stedi-health-analytics",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTable_node1693613652148",
)

job.commit()
