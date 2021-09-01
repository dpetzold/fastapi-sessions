from fastapi import FastAPI, Response

from fastapi_sessions.middleware.session import SessionDataMiddleware


app = FastAPI()

app.add_middleware(SessionDataMiddleware, secret_key="DONOTUSE")


@app.get("/create-session")
async def create_session(response: Response):
    return "created session"


@app.post("/create_session/{name}")
async def create_session_name(name: str, response: Response):
    return f"created session for {name}"


"""
@app.get("/whoami", dependencies=[Depends(cookie)])
async def whoami(session_data: SessionData = Depends(verifier)):
    return session_data


@app.post("/delete_session")
async def del_session(response: Response, session_id: UUID = Depends(cookie)):
    await backend.delete(session_id)
    cookie.delete_from_response(response)
    return "deleted session"
"""
