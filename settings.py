from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    token: str = Field(..., env="TOKEN")
    resume_id: str = Field(..., env="RESUME_ID")
    api_url: str = Field("https://api.hh.ru", env="API_URL")

    class Config:
        env_file = ".env"


settings = Settings()
