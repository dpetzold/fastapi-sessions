"""DynamoDB implementation."""
from dataclasses import dataclass
import datetime
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from typing import Generic

from config import settings

from fastapi_sessions.backends.session_backend import (
    BackendError,
    SessionBackend,
    SessionModel,
)
from fastapi_sessions.frontends.session_frontend import ID


@dataclass
class DynamoDbBackend(Generic[ID, SessionModel], SessionBackend[ID, SessionModel]):
    """Stores session data in a dictionary."""

    aws_region: str
    aws_profile_name: str
    table_name: str

    @property
    def aws_session(self):
        return boto3.session.Session(
            profile_name=self.aws_profile_name,
            region_name=self.aws_region,
        )

    @property
    def dynamodb_client(self):
        return self.aws_session.client("dynamodb")

    @property
    def dynamodb_resource(self):
        return self.aws_session.resource("dynamodb")

    @property
    def table(self):
        return self.dynamodb_resource.Table(self.table_name)

    @property
    def serializer(self):
        return boto3.dynamodb.types.TypeSerializer()

    def get(self, session_id):
        items = self.table.query(
            KeyConditionExpression=(Key("SessionId").eq(str(session_id))),
        )["Items"]
        if items:
            return items[0]
        return None

    def put(self, session_id, data: SessionModel = None):
        item = data.dict() if data else {}
        item.update(
            {
                "SessionId": str(session_id),
                "ttl": int(
                    (
                        datetime.datetime.utcnow() + datetime.timedelta(days=14)
                    ).timestamp()
                ),
            }
        )

        # Enforce uniqueness
        # https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/

        try:
            self.dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "ConditionExpression": "attribute_not_exists(SessionId)",
                            "Item": {
                                key: self.serializer.serialize(value)
                                for key, value in item.items()
                            },
                        }
                    },
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "ConditionExpression": "attribute_not_exists(SessionId)",
                            "Item": {
                                "SessionId": self.serializer.serialize(
                                    f"username#{item['username']}"
                                ),
                            },
                        }
                    },
                ]
            )
        except (
            ClientError,  # XXX: The correct exception catch is not working
            self.dynamodb_client.exceptions.TransactionCanceledException,
        ) as exc:
            if exc.response["Error"]["Code"] == "TransactionCanceledException":
                raise BackendError(f"username {item['username']} exists")
            raise exc

    async def create(self, session_id: ID, data: SessionModel):
        """Create a new session entry."""
        if self.get(session_id):
            raise BackendError("create can't overwrite an existing session")
        self.put(session_id, data)

    async def read(self, session_id: ID):
        """Read an existing session data."""
        return self.get(session_id)

    async def update(self, session_id: ID, data: SessionModel) -> None:
        """Update an existing session."""
        if self.get(session_id):
            self.put(session_id, data)
        raise BackendError("session does not exist, cannot update")

    async def delete(self, session_id: ID) -> None:
        """Delete the session"""
        session = self.get(session_id)
        if not session:
            raise BackendError("session does not exist, cannot delete")

        self.dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Delete": {
                        "TableName": self.table_name,
                        "Key": {
                            "SessionId": self.serializer.serialize(str(session_id))
                        },
                    },
                },
                {
                    "Delete": {
                        "TableName": self.table_name,
                        "Key": {
                            "SessionId": self.serializer.serialize(
                                f"username#{session['username']}"
                            )
                        },
                    }
                },
            ]
        )
