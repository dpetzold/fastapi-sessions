import logging
from pydantic import BaseModel
from fastapi import FastAPI, Response, Request

from fastapi_sessions.middleware.session import (
    SessionDataMiddleware,
)


app = FastAPI()


class LogConfig(BaseModel):
    """Logging configuration to be set for the server"""

    LOGGER_NAME: str = "fastapi_sessions"
    LOG_FORMAT: str = "%(levelprefix)s | %(asctime)s | %(message)s"
    LOG_LEVEL: str = "DEBUG"

    # Logging config
    version = 1
    disable_existing_loggers = False
    formatters = {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": LOG_FORMAT,
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    }
    handlers = {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    }
    loggers = {
        "fastapi_sessions": {"handlers": ["default"], "level": LOG_LEVEL},
    }


logging.config.dictConfig(LogConfig().dict())
logger = logging.getLogger(__name__)

app.add_middleware(SessionDataMiddleware, secret_key="DONOTUSE")


@app.get("/create-session")
async def create_session(request: Request, response: Response):
    if hasattr(request.state, "session_data"):
        logger.info("has session_data")

    return "created session"


@app.post("/create-session/{username}")
async def create_session_name(username: str, request: Request, response: Response):
    if hasattr(request.state, "session_data"):
        request.state.session_data.username = username

    return f"created session for {username}"


@app.get("/whoami")
async def whoami(request: Request):
    if hasattr(request.state, "session_data"):
        return request.state.session_data.dict()
    return {}


"""
@app.post("/delete-session")
async def del_session(response: Response, session_id: UUID = Depends(cookie)):
    await backend.delete(session_id)
    cookie.delete_from_response(response)
    return "deleted session"
"""
