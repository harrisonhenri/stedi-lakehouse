from typing import TypedDict, Union, Literal
from mypy_boto3_ec2.type_defs import ResponseMetadataTypeDef

Data = TypedDict(
    "Data",
    {
        "ResponseMetadata": ResponseMetadataTypeDef,
    },
)

Action = Union[Literal["CREATION"], Literal["DELETION"]]


def format_aws_http_response(data: Data, action: Action = "CREATION") -> None:
    http_status_code = data.get("ResponseMetadata").get("HTTPStatusCode")
    action_text = "created" if action == "CREATION" else "decommissioned"
    result = (
        f"{action_text} successfully"
        if 200 <= http_status_code < 300
        else f"not {action_text}"
    )

    print(f"*************Resource {result}*************")
