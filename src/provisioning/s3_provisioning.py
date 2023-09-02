from boto3 import client, resource
from mypy_boto3_ec2 import EC2Client
from mypy_boto3_s3 import S3ServiceResource

from src.config import AWS, LAKEHOUSE
from operator import itemgetter

from src.utils.format_aws_http_response import format_aws_http_response

REGION = AWS.get("REGION")
BUCKET_NAME = LAKEHOUSE.get("BUCKET_NAME")
SERVICE_NAME = f"com.amazonaws.{REGION}.s3"


def clients():
    s3 = resource("s3", region_name=REGION)
    ec2 = client("ec2", region_name=REGION)

    return {"s3": s3, "ec2": ec2}


def s3_provisioning():
    print("*********Provisioning S3*********")

    ec2: EC2Client = itemgetter("ec2")(clients())
    s3: S3ServiceResource = itemgetter("s3")(clients())

    s3.create_bucket(Bucket=BUCKET_NAME)

    vpc = ec2.describe_vpcs().get("Vpcs")[0]
    vpc_id = vpc.get("VpcId")

    routing_table = ec2.describe_route_tables().get("RouteTables")[0]
    routing_table_id = routing_table.get("RouteTableId")

    format_aws_http_response(
        ec2.create_vpc_endpoint(
            VpcId=vpc_id, ServiceName=SERVICE_NAME, RouteTableIds=[routing_table_id]
        )
    )
