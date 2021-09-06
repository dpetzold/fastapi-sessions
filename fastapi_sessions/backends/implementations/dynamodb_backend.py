"""DynamoDB implementation."""
import logging
from dataclasses import dataclass
import datetime
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from typing import Generic

from fastapi_sessions.backends.session_backend import (
    BackendError,
    SessionBackend,
    SessionModel,
)
from fastapi_sessions.frontends.session_frontend import ID


logger = logging.getLogger(__name__)


@dataclass
class DynamoDbBackend(Generic[ID, SessionModel], SessionBackend[ID, SessionModel]):
    """Stores session data in a dictionary."""

    aws_region: str
    aws_profile_name: str
    table_name: str

    service_name: str = "dynamodb"
    partition_key: str = "session_id"

    @property
    def aws_session(self):
        return boto3.session.Session(
            profile_name=self.aws_profile_name,
            region_name=self.aws_region,
        )

    @property
    def dynamodb_client(self):
        return self.aws_session.client(self.service_name)

    @property
    def dynamodb_resource(self):
        return self.aws_session.resource(self.service_name)

    @property
    def table(self):
        return self.dynamodb_resource.Table(self.table_name)

    @property
    def serializer(self):
        return boto3.dynamodb.types.TypeSerializer()

    def write_items(self, transact_items):
        logger.debug(transact_items)

        try:
            self.dynamodb_client.transact_write_items(
                TransactItems=transact_items,
            )
        except (
            ClientError,  # XXX: The correct exception catch is not working
            self.dynamodb_client.exceptions.TransactionCanceledException,
        ) as exc:
            if exc.response["Error"]["Code"] == "TransactionCanceledException":
                raise BackendError("username exists")
            raise exc

    def put(self, session_id, data: SessionModel = None):
        item = data.dict() if data else {}
        item.update(
            {
                self.partition_key: str(session_id),
                "ttl": int(
                    (
                        datetime.datetime.utcnow() + datetime.timedelta(days=14)
                    ).timestamp()
                ),
            }
        )

        # Enforce uniqueness
        # https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/

        condition_expression = f"attribute_not_exists({self.partition_key})"

        transact_items = [
            {
                "Put": {
                    "TableName": self.table_name,
                    "ConditionExpression": condition_expression,
                    "Item": {
                        key: self.serializer.serialize(value)
                        for key, value in item.items()
                    },
                }
            },
        ]

        """
        username = item.get("username")
        if username:
            user_item = self._get("username", username)
            logger.debug(user_item)
            if not user_item:
                transact_items.append(
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "ConditionExpression": condition_expression,
                            "Item": {
                                "session_id": self.serializer.serialize(
                                    user_session_id
                                ),
                            },
                        }
                    }
                )
        """

        self.write_items(transact_items)

    def _get(self, key, value):
        items = self.table.query(
            KeyConditionExpression=(Key(key).eq(str(value))),
        )["Items"]
        if items:
            return items[0]
        return None

    def get(self, value):
        return self._get(self.partition_key, value)

    async def create(self, session_id: ID, data: SessionModel = None):
        """Create a new session entry."""
        if self._get(self.partition_key, session_id):
            raise BackendError("create can't overwrite an existing session")
        self.put(session_id, data)

    async def read(self, session_id: ID):
        """Read an existing session data."""
        return self._get(self.partition_key, session_id)

    async def update(self, session_id: ID, data: SessionModel) -> None:
        """Update an existing session."""
        item = data.dict() if data else {}
        item.update(
            {
                self.partition_key: str(session_id),
                "ttl": int(
                    (
                        datetime.datetime.utcnow() + datetime.timedelta(days=14)
                    ).timestamp()
                ),
            }
        )

        # Enforce uniqueness
        # https://aws.amazon.com/blogs/database/simulating-amazon-dynamodb-unique-constraints-using-transactions/

        update_expression = ", ".join(
            [
                f"{key} = :{key}"
                for key in item.keys()
                if key not in (self.partition_key, "ttl")
            ]
        )

        expression_attribute_values = {
            f":{key}": self.serializer.serialize(value)
            for key, value in item.items()
            if key not in (self.partition_key, "ttl")
        }

        transact_items = [
            {
                "Update": {
                    "TableName": self.table_name,
                    "Key": {self.partition_key: {"S": session_id}},
                    "ConditionExpression": "attribute_not_exists(username)",
                    "UpdateExpression": f"SET {update_expression}",
                    "ExpressionAttributeValues": expression_attribute_values,
                }
            },
        ]

        """
        if item.get("username"):
            user_item = self.get("username", username)
            logger.debug(user_item)
            if not user_item:
                transact_items.append(
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "Item": {
                                "session_id": self.serializer.serialize(
                                    user_session_id
                                ),
                            },
                        }
                    }
                )
        """

        self.write_items(transact_items)

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
                            self.partition_key: self.serializer.serialize(
                                str(session_id)
                            )
                        },
                    },
                },
                {
                    "Delete": {
                        "TableName": self.table_name,
                        "Key": {
                            self.partition_key: self.serializer.serialize(
                                f"username#{session['username']}"
                            )
                        },
                    }
                },
            ]
        )
