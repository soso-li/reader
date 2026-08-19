from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

AI_MODEL_NAME_MAX_LENGTH = 120


class Settings(BaseSettings):
    database_url: str = Field(validation_alias="DATABASE_URL")
    redis_url: str = Field(default="redis://localhost:6387/0", validation_alias="REDIS_URL")
    rsshub_base_url: str = Field(default="", validation_alias="RSSHUB_BASE_URL")
    llm_base_url: str = Field(default="http://127.0.0.1:1234", validation_alias="LLM_BASE_URL")
    llm_model: str = Field(
        default="qwen/qwen3.5-9b",
        max_length=AI_MODEL_NAME_MAX_LENGTH,
        validation_alias="LLM_MODEL",
    )
    translation_provider: str = Field(default="local", validation_alias="TRANSLATION_PROVIDER")
    translation_base_url: str = Field(default="", validation_alias="TRANSLATION_BASE_URL")
    translation_model: str = Field(default="hy-mt2-1.8b", validation_alias="TRANSLATION_MODEL")
    embedding_base_url: str = Field(default="http://127.0.0.1:1234", validation_alias="EMBEDDING_BASE_URL")
    embedding_model: str = Field(default="text-embedding-qwen3-embedding-4b", validation_alias="EMBEDDING_MODEL")
    embedding_api_key: str = Field(default="", validation_alias="EMBEDDING_API_KEY")
    llm_timeout_seconds: float = Field(default=240.0, validation_alias="LLM_TIMEOUT_SECONDS")
    rq_job_timeout_seconds: int = Field(default=3600, validation_alias="RQ_JOB_TIMEOUT_SECONDS")
    rss_fetch_interval_seconds: int = Field(default=1200, ge=60, validation_alias="RSS_FETCH_INTERVAL_SECONDS")
    llm_task_provider: str = Field(default="local", validation_alias="LLM_TASK_PROVIDER")
    api_token: str = Field(default="", validation_alias="READER_API_TOKEN")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
