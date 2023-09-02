import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Bucket
S3Bucket_node1693580056473 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-health-analytics-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3Bucket_node1693580056473",
)

# Script generated for node Filter
Filter_node1693580467237 = Filter.apply(
    frame=S3Bucket_node1693580056473,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1693580467237",
)

# Script generated for node Glue Table
GlueTable_node1693581869338 = glueContext.write_dynamic_frame.from_catalog(
    frame=Filter_node1693580467237,
    database="stedi-health-analytics",
    table_name="customer_trusted",
    transformation_ctx="GlueTable_node1693581869338",
)

job.commit()
