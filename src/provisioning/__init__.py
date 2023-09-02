from src.provisioning.glue_role_provisioning import glue_role_provisioning
from src.provisioning.s3_provisioning import s3_provisioning

if __name__ == "__main__":
    s3_provisioning()
    glue_role_provisioning()
