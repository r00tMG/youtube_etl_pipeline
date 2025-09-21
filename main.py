import os

from dotenv import load_dotenv
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from app import models, database
from router import youtube

load_dotenv()

SECRET_KEY_MIDDLEWARE = os.getenv("SECRET_KEY_MIDDLEWARE")

models.Base.metadata.create_all(bind=database.engine)

app = FastAPI()


app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY_MIDDLEWARE)

app.include_router(youtube.router)
