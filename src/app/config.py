from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    DART_API_KEY: str | None = None

    # MVP: r(요구수익률) 수동 입력(분기 1회)
    DEFAULT_DISCOUNT_RATE: float = 0.10

    class Config:
        env_file = ".env"

settings = Settings()
