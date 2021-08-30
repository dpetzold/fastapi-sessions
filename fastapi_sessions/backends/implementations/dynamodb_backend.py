"""InMemoryBackend implementation."""
import boto3
from boto3.dynamodb.conditions import Key

from typing import Generic

from config import settings

from fastapi_sessions.backends.session_backend import (
    BackendError,
    SessionBackend,
    SessionModel,
)
from fastapi_sessions.frontends.session_frontend import ID


class DynamoDbBackend(Generic[ID, SessionModel], SessionBackend[ID, SessionModel]):
    """Stores session data in a dictionary."""

    @property
    def aws_session(self):
        return boto3.session.Session(profile_name=settings.get("AWS_PROFILE_NAME"))

    def aws_client(self, service):
        return self.aws_session.client(service)

    @property
    def dynamodb_client(self):
        return self.aws_client("dynamodb")

    @property
    def resource(self):
        return boto3.resource("dynamodb", region_name=settings.get("AWS_REGION"))

    @property
    def table(self):
        return self.resource.Table(self.table_name)

    def get(self, session_id):
        return self.table.query(
            KeyConditionExpression=(Key("session_id").eq(session_id)),
        )["Items"]

    def put(self, session_id, data: SessionModel = None):
        item = data.dict() if data else {}
        item.update({"SessionId": session_id})
        self.table.put_item(Item=item)

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
        return self.table.delete_item(Key={"SessionId": session_id})
