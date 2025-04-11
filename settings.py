import re
from datetime import datetime
from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    hh_token: str = Field(..., env="HH_TOKEN")
    resume_id: str = Field(..., env="RESUME_ID")
    hh_api_url: str = Field("https://api.hh.ru", env="HH_API_URL")
    notion_api_url: str = Field("https://api.notion.com/v1", env="NOTION_API_URL")
    notion_secret: str = Field("", env="NOTION_SECRET")
    notion_db_id: str = Field("", env="NOTION_DB_ID")
    notion_proxy: str | None = Field(None, env="NOTION_PROXY")
    notion_resume_id: str = Field("", env="NOTION_RESUME_ID")

    @computed_field
    def vacancies_url(self) -> str:
        return (
            f"{self.hh_api_url.rstrip('/')}/resumes/{self.resume_id}/similar_vacancies"
        )

    @computed_field
    def negotiation_url(self) -> str:
        return f"{self.hh_api_url.rstrip('/')}/negotiations"

    @computed_field
    def hh_headers(self) -> dict:
        return {"Authorization": f"Bearer {self.hh_token}"}

    @computed_field
    def notion_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.notion_secret}",
            "Notion-Version": "2022-06-28",
        }

    @computed_field
    def notion_apply_date(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    @computed_field
    def notion_enabled(self) -> bool:
        return self.notion_db_id and self.notion_secret

    @computed_field
    def cover_letter(self) -> str:
        path = Path("cover_letter.txt")
        return path.read_text() if path.exists() else ""

    @computed_field
    def blacklist(self) -> str:
        path = Path("blacklist.txt")
        return (
            set(map(str.lower, path.read_text().splitlines())) if path.exists() else ""
        )

    @computed_field
    def blacklist_regex(self) -> str:
        return re.compile(r"\b[0-9а-яa-z]+\b")

    class Config:
        env_file = ".env"
        extra = "ignore"
        frozen = True


settings = Settings()
