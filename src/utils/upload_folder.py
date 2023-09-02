import os
from mypy_boto3_s3.service_resource import Bucket


def upload_folder(bucket: Bucket, prefix: str):
    try:
        root_path = os.getcwd()
        full_path = f"{root_path}/"

        for path, _, files in os.walk(full_path):
            directory_name = path.replace(full_path, "")
            for file in files:
                if "py" in file:
                    break

                key = f"{directory_name}/{prefix}/{file}"
                bucket.upload_file(os.path.join(path, file), key)

    except Exception as err:
        print(err)
