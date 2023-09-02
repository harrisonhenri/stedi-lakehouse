from boto3 import client, resource
from mypy_boto3_ec2 import EC2Client
from mypy_boto3_s3 import S3ServiceResource, S3Client

from src.config import AWS, LAKEHOUSE
from operator import itemgetter

from src.utils.format_aws_http_response import format_aws_http_response

REGION = AWS.get("REGION")
BUCKET_NAME = LAKEHOUSE.get("BUCKET_NAME")
SERVICE_NAME = f"com.amazonaws.{REGION}.s3"


def clients():
    s3 = client("s3", region_name=REGION)
    s3_resource = resource("s3", region_name=REGION)
    ec2 = client("ec2", region_name=REGION)

    return {"s3": s3, "ec2": ec2, "s3_resource": s3_resource}


def s3_decommissioning():
    print("*********Decommissioning S3*********")

    ec2: EC2Client = itemgetter("ec2")(clients())
    s3: S3Client = itemgetter("s3")(clients())
    s3_resource: S3ServiceResource = itemgetter("s3_resource")(clients())

    s3_resource.Bucket(BUCKET_NAME).objects.all().delete()
    s3.delete_bucket(Bucket=BUCKET_NAME)

    vpc_endpoint_id = (
        ec2.describe_vpc_endpoints().get("VpcEndpoints")[0].get("VpcEndpointId")
    )
    format_aws_http_response(
        ec2.delete_vpc_endpoints(VpcEndpointIds=[vpc_endpoint_id]), action="DELETION"
    )
