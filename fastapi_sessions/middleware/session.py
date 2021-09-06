import logging
import typing
from decimal import Decimal
from dataclasses import dataclass
from uuid import UUID, uuid4

from fastapi import HTTPException, Request, Response

from pydantic import BaseModel

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp

from fastapi_sessions.backends.session_backend import BackendError
from fastapi_sessions.backends.implementations import DynamoDbBackend
from fastapi_sessions.frontends.implementations import (
    CookieParameters,
    SessionCookie,
)
from fastapi_sessions.session_verifier import SessionVerifier

from config import settings


logger = logging.getLogger(__name__)
logger.info(__name__)


class SessionData(BaseModel):
    session_id: str
    username: typing.Optional[str] = None
    ttl: Decimal = None


class BasicVerifier(SessionVerifier[UUID, SessionData]):
    def __init__(
        self,
        *,
        identifier: str,
        auto_error: bool,
        backend: DynamoDbBackend[UUID, SessionData],
        auth_http_exception: HTTPException,
    ):
        self._identifier = identifier
        self._auto_error = auto_error
        self._backend = backend
        self._auth_http_exception = auth_http_exception

    @property
    def identifier(self):
        return self._identifier

    @property
    def backend(self):
        return self._backend

    @property
    def auto_error(self):
        return self._auto_error

    @property
    def auth_http_exception(self):
        return self._auth_http_exception

    def verify_session(self, model: SessionData) -> bool:
        """If the session exists, it is valid"""
        return True


@dataclass
class SessionDataMiddleware(BaseHTTPMiddleware):

    app: ASGIApp
    secret_key: str
    max_age: int = 14 * 24 * 60 * 60  # 14 days, in seconds
    same_site: str = "lax"
    https_only: bool = False
    cookie_name = "session-data"
    identifier = "general_verifier"

    def __post_init__(self):
        super().__init__(self.app)
        self.security_flags = "httponly; samesite=" + self.same_site
        if self.https_only:  # Secure flag can be used with HTTPS only
            self.security_flags += "; secure"
        self.cookie_params = CookieParameters()

    @property
    def verifier(self):
        return BasicVerifier(
            identifier=self.identifier,
            auto_error=True,
            backend=self.backend,
            auth_http_exception=HTTPException(
                status_code=403, detail="invalid session"
            ),
        )

    @property
    def cookie(self):
        return SessionCookie(
            cookie_name=self.cookie_name,
            identifier=self.identifier,
            auto_error=True,
            secret_key=self.secret_key,
            cookie_params=self.cookie_params,
        )

    @property
    def backend(self):
        return DynamoDbBackend[UUID, SessionData](
            aws_region=settings.AWS_REGION,
            aws_profile_name=settings.get("AWS_PROFILE_NAME"),
            table_name=settings.DYNAMODB_SESSION_TABLE_NAME,
        )

    async def create_session(self, request: Request, response: Response):
        session = uuid4()

        try:
            await self.backend.create(session)
        except BackendError as exc:
            return str(exc)

        request.state.session_data = SessionData(
            session_id=str(session),
        )

        self.cookie.attach_to_response(response, session)
        logger.info(f"Created session {str(session)}")

    async def get_session(self, request: Request):
        session_id = self.cookie(request)
        logger.info(session_id)
        data = self.backend.get(session_id)

        logger.info(data)

        request.state.session_data = SessionData(
            **data,
        )

        logger.info(request.state.session_data)

    async def save_session(self, session_id: str, session_data: SessionData):
        self.backend.update(session_id, session_data)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:

        cookie = request.cookies.get(self.cookie_name)
        if cookie:
            await self.get_session(request)

        response = await call_next(request)

        if not cookie:
            await self.create_session(request, response)

        return response
