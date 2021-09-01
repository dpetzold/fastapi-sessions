from dataclasses import dataclass
from uuid import UUID, uuid4

from fastapi import HTTPException

from pydantic import BaseModel

from starlette.types import ASGIApp, Message, Receive, Scope, Send
from starlette.datastructures import MutableHeaders

from fastapi_sessions.backends.session_backend import BackendError
from fastapi_sessions.backends.implementations import DynamoDbBackend
from fastapi_sessions.frontends.implementations import (
    CookieParameters,
    SessionCookie,
)
from fastapi_sessions.session_verifier import SessionVerifier

from config import settings


class SessionData(BaseModel):
    username: str


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
class SessionDataMiddleware:

    app: ASGIApp
    secret_key: str
    session_cookie: str = "session"
    max_age: int = 14 * 24 * 60 * 60  # 14 days, in seconds
    same_site: str = "lax"
    https_only: bool = False
    cookie_name = "cookie"

    def __post_init__(self):
        self.security_flags = "httponly; samesite=" + self.same_site
        if self.https_only:  # Secure flag can be used with HTTPS only
            self.security_flags += "; secure"
        self.cookie_params = CookieParameters()

    @property
    def verifier(self):
        return BasicVerifier(
            identifier="general_verifier",
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
            identifier="general_verifier",
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

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:

        if scope["type"] not in ("http"):  # pragma: no cover
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":

                session = uuid4()

                try:
                    await self.backend.create(session)
                except BackendError as exc:
                    return str(exc)

                headers = MutableHeaders(scope=message)
                headers.set_cookie(
                    key=self.cookie_name,
                    value=str(self.cookie.signer.dumps(session.hex)),
                    **dict(self.cookie.cookie_params),
                )

                # self.cookie.attach_to_response(scope, session)

            await send(message)

        await self.app(scope, receive, send_wrapper)
