from boto3 import client
from src.config import AWS, LAKEHOUSE
from src.utils.format_aws_http_response import format_aws_http_response

REGION = AWS.get("REGION")
BUCKET_NAME = LAKEHOUSE.get("BUCKET_NAME")
BUCKET_ARN = f"arn:aws:s3:::{BUCKET_NAME}"


GLUE_ROLE = LAKEHOUSE.get("GLUE_ROLE")
GLUE_ROLE_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}"""

GLUE_ROLE_S3_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListObjectsInBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::_______"
      ]
    },
    {
      "Sid": "AllObjectActions",
      "Effect": "Allow",
      "Action": "s3:*Object",
      "Resource": [
        "arn:aws:s3:::_______/*"
      ]
    }
  ]
}""".replace(
    "arn:aws:s3:::_______", BUCKET_ARN
)

GLUE_ROLE_GENERAL_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }
    ]
}"""


def glue_role_provisioning():
    print("*********Provisioning Glue Role*********")

    iam = client("iam", region_name=REGION)

    iam.create_role(RoleName=GLUE_ROLE, AssumeRolePolicyDocument=GLUE_ROLE_POLICY)
    iam.put_role_policy(
        RoleName=GLUE_ROLE, PolicyName="S3Access", PolicyDocument=GLUE_ROLE_S3_POLICY
    )
    format_aws_http_response(
        iam.put_role_policy(
            RoleName=GLUE_ROLE,
            PolicyName="GlueAccess",
            PolicyDocument=GLUE_ROLE_GENERAL_POLICY,
        )
    )
