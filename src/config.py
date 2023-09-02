import os

from decouple import config

os.environ["AWS_PROFILE"] = config("AWS_PROFILE")


AWS = {
    "REGION": config("REGION"),
}

LAKEHOUSE = {"BUCKET_NAME": config("BUCKET_NAME"), "GLUE_ROLE": config("GLUE_ROLE")}
