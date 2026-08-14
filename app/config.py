import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    API_KEY: str = ""
    WEBHOOK_SECRET: str = ""

    BASE_URL: str = "https://pseudogram-api.onrender.com"
    DATABASE_URL: str = "sqlite:///./linkplease.db"

    model_config = {
        "env_file": ".env",
        "extra": "ignore",
    }


settings = Settings()
