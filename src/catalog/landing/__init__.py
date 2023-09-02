from boto3 import resource
from src.config import AWS, LAKEHOUSE
from src.utils.upload_folder import upload_folder

REGION = AWS.get("REGION")
BUCKET_NAME = LAKEHOUSE.get("BUCKET_NAME")
SERVICE_NAME = f"com.amazonaws.{REGION}.s3"


def landing_data():
    print("*********Loading landing data*********")

    s3 = resource("s3", region_name=REGION)
    bucket = s3.Bucket(BUCKET_NAME)
    prefix = "landing"

    upload_folder(bucket, prefix)
    print("*************Data loaded*************")


def main():
    landing_data()


if __name__ == "__main__":
    main()
