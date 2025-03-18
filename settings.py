from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    token: str = Field(..., env="TOKEN")
    resume_id: str = Field(..., env="RESUME_ID")
    api_url: str = Field("https://api.hh.ru", env="API_URL")
    notion_api_url: str = Field("https://api.notion.com/v1", env="NOTION_API_URL")
    notion_secret: str = Field("", env="NOTION_SECRET")
    notion_db_id: str = Field("", env="NOTION_DB_ID")
    notion_proxy: str | None = Field(None, env="NOTION_PROXY")

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
