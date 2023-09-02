from boto3 import client
from src.config import LAKEHOUSE, AWS
from src.utils.format_aws_http_response import format_aws_http_response

GLUE_ROLE = LAKEHOUSE.get("GLUE_ROLE")
REGION = AWS.get("REGION")


def glue_role_decommissioning():
    print("*********Decommissioning Glue Role*********")

    iam = client("iam", region_name=REGION)

    format_aws_http_response(iam.delete_role(RoleName=GLUE_ROLE), action="DELETION")
