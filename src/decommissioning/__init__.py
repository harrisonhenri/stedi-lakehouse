from src.decommissioning.glue_role_decommissioning import glue_role_decommissioning
from src.decommissioning.s3_decommissioning import s3_decommissioning

if __name__ == "__main__":
    s3_decommissioning()
    glue_role_decommissioning()
